use super::managed_tools::{ActivityGuard, MAX_OUTPUT, Managed};
use agent::{AgentTool, ToolCallEventStream, ToolInput};
use agent_client_protocol::schema::v1 as acp;
use anyhow::{Context as _, Result, ensure};
use gpui::{App, AppContext as _, SharedString, Task};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    ffi::{CStr, CString},
    fs::{File, OpenOptions, Permissions},
    io::{Read, Write},
    os::{
        fd::{AsRawFd, FromRawFd, IntoRawFd},
        unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    },
    path::{Component, Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FileOp {
    Read,
    List,
    Find,
    Grep,
    Write,
    Edit,
    CreateDirectory,
    Move,
    Delete,
}
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FileInput {
    pub op: FileOp,
    pub path: String,
    pub target: Option<String>,
    pub content: Option<String>,
    pub old_text: Option<String>,
    pub new_text: Option<String>,
    pub pattern: Option<String>,
}

pub struct ManagedFiles {
    managed: Arc<Managed>,
    root: File,
    output_parent: File,
}
impl ManagedFiles {
    pub fn new(managed: Arc<Managed>) -> Result<Self> {
        let open = |p: &Path| {
            OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(p)
        };
        let root = open(&managed.workdir)?;
        let output_parent = open(managed.output.parent().context("output parent required")?)?;
        Ok(Self {
            managed,
            root,
            output_parent,
        })
    }
    fn path(&self, input: &str) -> Result<(&File, PathBuf)> {
        let path = Path::new(input);
        let (root, relative) = if path.is_absolute() {
            if path == self.managed.output {
                (
                    &self.output_parent,
                    PathBuf::from(path.file_name().context("output name required")?),
                )
            } else {
                (
                    &self.root,
                    path.strip_prefix(&self.managed.workdir)
                        .context("path outside assignment")?
                        .to_path_buf(),
                )
            }
        } else {
            (&self.root, path.to_path_buf())
        };
        for component in relative.components() {
            match component {
                Component::Normal(name) => {
                    ensure!(!protected(&name.to_string_lossy()), "protected path")
                }
                Component::CurDir => {}
                _ => anyhow::bail!("non-local path"),
            }
        }
        Ok((root, relative))
    }
    fn parent(&self, input: &str) -> Result<(File, CString)> {
        let (root, relative) = self.path(input)?;
        let name = CString::new(
            relative
                .file_name()
                .context("file required")?
                .as_encoded_bytes(),
        )?;
        let mut directory = root.try_clone()?;
        for component in relative.parent().unwrap_or(Path::new("")).components() {
            if let Component::Normal(name) = component {
                directory = open_at(&directory, &CString::new(name.as_encoded_bytes())?, true)?;
            }
        }
        Ok((directory, name))
    }
    fn open(&self, input: &str) -> Result<File> {
        let (root, relative) = self.path(input)?;
        if relative.as_os_str().is_empty() || relative == Path::new(".") {
            return Ok(root.try_clone()?);
        }
        let (parent, name) = self.parent(input)?;
        open_at(&parent, &name, false)
    }
    pub fn execute(&self, input: FileInput) -> Result<String> {
        let string = |v: Option<String>| v.context("missing tool field");
        let result = match input.op {
            FileOp::Read => read_text(self.open(&input.path)?)?,
            FileOp::List => names(&self.open(&input.path)?)?.join("\n"),
            FileOp::Find | FileOp::Grep => {
                let pattern = string(input.pattern)?;
                ensure!(!pattern.is_empty(), "empty pattern");
                let mut files = Vec::new();
                walk(self.open(&input.path)?, PathBuf::new(), 0, &mut files)?;
                let mut matches = Vec::new();
                for relative in files {
                    if matches.len() >= 500 {
                        break;
                    }
                    if matches!(input.op, FileOp::Find) {
                        if relative.to_string_lossy().contains(&pattern) {
                            matches.push(relative.to_string_lossy().into_owned());
                        }
                    } else {
                        let path = Path::new(&input.path).join(&relative);
                        if let Ok(file) = self.open(path.to_str().context("UTF-8 path required")?)
                            && let Ok(text) = read_text(file)
                        {
                            for (line, text) in text.lines().enumerate() {
                                if text.contains(&pattern) {
                                    matches.push(format!(
                                        "{}:{}:{}",
                                        relative.display(),
                                        line + 1,
                                        text
                                    ));
                                    if matches.len() >= 500 {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                matches.join("\n")
            }
            FileOp::Write => {
                self.write(&input.path, &string(input.content)?)?;
                "written".into()
            }
            FileOp::Edit => {
                let old = string(input.old_text)?;
                let new = string(input.new_text)?;
                let current = read_text(self.open(&input.path)?)?;
                ensure!(
                    !old.is_empty() && current.matches(&old).count() == 1,
                    "edit must match exactly once"
                );
                self.write(&input.path, &current.replacen(&old, &new, 1))?;
                "edited".into()
            }
            FileOp::CreateDirectory => {
                ensure!(
                    !self.is_output(&input.path),
                    "output artifact must be a file"
                );
                let (parent, name) = self.parent(&input.path)?;
                syscall(unsafe { libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), 0o755) })?;
                "created".into()
            }
            FileOp::Move => {
                // Inspect the source without following its final component before rename.
                let source = self.open(&input.path)?;
                ensure!(
                    source.metadata()?.is_file() || source.metadata()?.is_dir(),
                    "regular entry required"
                );
                let (source_parent, source_name) = self.parent(&input.path)?;
                let target = string(input.target)?;
                ensure!(
                    !self.is_output(&target) || source.metadata()?.is_file(),
                    "output artifact must be a file"
                );
                let (target_parent, target_name) = self.parent(&target)?;
                syscall(unsafe {
                    libc::renameat(
                        source_parent.as_raw_fd(),
                        source_name.as_ptr(),
                        target_parent.as_raw_fd(),
                        target_name.as_ptr(),
                    )
                })?;
                "moved".into()
            }
            FileOp::Delete => {
                let file = self.open(&input.path)?;
                let is_dir = file.metadata()?.is_dir();
                let (parent, name) = self.parent(&input.path)?;
                syscall(unsafe {
                    libc::unlinkat(
                        parent.as_raw_fd(),
                        name.as_ptr(),
                        if is_dir { libc::AT_REMOVEDIR } else { 0 },
                    )
                })?;
                "deleted".into()
            }
        };
        ensure!(result.len() <= MAX_OUTPUT, "file output too large");
        Ok(result)
    }
    fn is_output(&self, path: &str) -> bool {
        let path = Path::new(path);
        path == self.managed.output
            || (!path.is_absolute() && self.managed.workdir.join(path) == self.managed.output)
    }
    fn write(&self, path: &str, text: &str) -> Result<()> {
        ensure!(text.len() <= MAX_OUTPUT, "file input too large");
        let (parent, name) = self.parent(path)?;
        let mut metadata = std::mem::MaybeUninit::<libc::stat>::uninit();
        let status = unsafe {
            libc::fstatat(
                parent.as_raw_fd(),
                name.as_ptr(),
                metadata.as_mut_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        };
        if status == 0 {
            ensure!(
                unsafe { metadata.assume_init() }.st_mode & libc::S_IFMT != libc::S_IFLNK,
                "symlink write rejected"
            );
        } else {
            ensure!(
                std::io::Error::last_os_error().raw_os_error() == Some(libc::ENOENT),
                "file metadata unavailable"
            );
        }
        // Atomic replacement never modifies an inode hard-linked outside the assignment.
        let mode = match self.open(path) {
            Ok(file) => {
                ensure!(file.metadata()?.is_file(), "regular file required");
                file.metadata()?.permissions().mode() & 0o777
            }
            Err(_) => 0o644,
        };
        static COUNT: AtomicU64 = AtomicU64::new(0);
        let temp = CString::new(format!(
            ".managed-{}-{}.tmp",
            std::process::id(),
            COUNT.fetch_add(1, Ordering::SeqCst)
        ))?;
        let fd = unsafe {
            libc::openat(
                parent.as_raw_fd(),
                temp.as_ptr(),
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                0o600,
            )
        };
        ensure!(fd >= 0, "file creation failed");
        let mut file = unsafe { File::from_raw_fd(fd) };
        let result = (|| {
            file.write_all(text.as_bytes())?;
            file.set_permissions(Permissions::from_mode(mode))?;
            syscall(unsafe {
                libc::renameat(
                    parent.as_raw_fd(),
                    temp.as_ptr(),
                    parent.as_raw_fd(),
                    name.as_ptr(),
                )
            })?;
            Ok(())
        })();
        if result.is_err() {
            syscall(unsafe { libc::unlinkat(parent.as_raw_fd(), temp.as_ptr(), 0) })?;
        }
        result
    }
}
fn syscall(result: i32) -> Result<()> {
    if result < 0 {
        Err(std::io::Error::last_os_error().into())
    } else {
        Ok(())
    }
}
fn open_at(parent: &File, name: &CStr, directory: bool) -> Result<File> {
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY
                | libc::O_NOFOLLOW
                | libc::O_CLOEXEC
                | libc::O_NONBLOCK
                | if directory { libc::O_DIRECTORY } else { 0 },
        )
    };
    ensure!(fd >= 0, "entry unavailable or symlink rejected");
    let file = unsafe { File::from_raw_fd(fd) };
    ensure!(
        file.metadata()?.is_file() || file.metadata()?.is_dir(),
        "regular entry required"
    );
    Ok(file)
}
fn read_text(file: File) -> Result<String> {
    ensure!(
        file.metadata()?.is_file() && file.metadata()?.nlink() == 1,
        "single-link regular file required"
    );
    let mut bytes = Vec::new();
    file.take((MAX_OUTPUT + 1) as u64).read_to_end(&mut bytes)?;
    ensure!(bytes.len() <= MAX_OUTPUT, "file too large");
    Ok(String::from_utf8(bytes)?)
}
fn names(directory: &File) -> Result<Vec<String>> {
    ensure!(directory.metadata()?.is_dir(), "directory required");
    let fd = open_at(directory, &CString::new(".")?, true)?.into_raw_fd();
    let dir = unsafe { libc::fdopendir(fd) };
    if dir.is_null() {
        unsafe { libc::close(fd) };
        anyhow::bail!("directory unavailable")
    }
    unsafe { libc::rewinddir(dir) };
    let mut names = Vec::new();
    loop {
        let entry = unsafe { libc::readdir(dir) };
        if entry.is_null() {
            break;
        }
        let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) }
            .to_str()
            .map(str::to_owned);
        if let Ok(name) = name
            && ![".", ".."].contains(&name.as_str())
            && !protected(&name)
        {
            names.push(name);
        }
        if names.len() > 2000 {
            break;
        }
    }
    syscall(unsafe { libc::closedir(dir) })?;
    ensure!(names.len() <= 2000, "directory too large");
    names.sort();
    Ok(names)
}
fn walk(directory: File, prefix: PathBuf, depth: usize, files: &mut Vec<PathBuf>) -> Result<()> {
    ensure!(depth < 32 && files.len() <= 2000, "search limit exceeded");
    for name in names(&directory)? {
        let path = prefix.join(&name);
        let child = open_at(&directory, &CString::new(name)?, false);
        if let Ok(child) = child {
            if child.metadata()?.is_dir() {
                walk(child, path, depth + 1, files)?;
            } else {
                files.push(path);
                ensure!(files.len() <= 2000, "search limit exceeded");
            }
        }
    }
    Ok(())
}
impl AgentTool for ManagedFiles {
    type Input = FileInput;
    type Output = String;
    const NAME: &'static str = "managed_files";
    fn description() -> SharedString {
        "Confined file operations: read, list, find (literal path substring), grep (literal text), write, exact edit, create_directory, move, delete (files/empty directories). Paths stay in the assigned worktree or exact assigned output file. No Git metadata, symlinks, or brain access; use factory_helper brain_read for knowledge.".into()
    }
    fn kind() -> acp::ToolKind {
        acp::ToolKind::Other
    }
    fn initial_title(&self, _: Result<FileInput, Value>, _: &mut App) -> SharedString {
        Self::NAME.into()
    }
    fn run(
        self: Arc<Self>,
        input: ToolInput<FileInput>,
        _: ToolCallEventStream,
        cx: &mut App,
    ) -> Task<Result<String, String>> {
        cx.spawn(async move |cx| {
            let input = input
                .recv()
                .await
                .map_err(|_| "invalid_tool_input".to_string())?;
            if self
                .managed
                .activity
                .lock()
                .expect("activity lock")
                .uncertain
            {
                return Err("managed_host_held".into());
            }
            let guard = ActivityGuard::new(self.managed.activity.clone());
            cx.background_spawn(async move {
                let result = self
                    .execute(input)
                    .map_err(|_| "file_policy_or_io_error".to_string());
                guard.complete();
                result
            })
            .await
        })
    }
}

fn protected(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    name.starts_with(".env")
        || [".git", ".ssh", ".gnupg", ".aws", ".netrc", ".docker"].contains(&name.as_str())
}
