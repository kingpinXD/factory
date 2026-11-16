package gh

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type EffectPRBranch struct {
	Ref  string `json:"ref"`
	SHA  string `json:"sha"`
	Repo struct { FullName string `json:"full_name"` } `json:"repo"`
}

type EffectPullRequest struct {
	ID     int64          `json:"id"`
	NodeID string         `json:"node_id"`
	Number int            `json:"number"`
	URL    string         `json:"html_url"`
	State  string         `json:"state"`
	Draft  bool           `json:"draft"`
	User   User           `json:"user"`
	Title  string         `json:"title"`
	Body   string         `json:"body"`
	Head   EffectPRBranch `json:"head"`
	Base   EffectPRBranch `json:"base"`
}

func (p *EffectPullRequest) UnmarshalJSON(data []byte) error {
	type plain EffectPullRequest
	var value plain
	if err:=json.Unmarshal(data,&value);err!=nil{return err}
	var fields map[string]json.RawMessage
	if err:=json.Unmarshal(data,&fields);err!=nil{return err}
	for _,key:=range []string{"draft","body","head","base"}{if raw,ok:=fields[key];!ok||bytes.Equal(raw,[]byte("null")){return fmt.Errorf("missing PR %s",key)}}
	if value.ID<1||value.NodeID==""||value.Number<1||value.URL==""||value.User.Login==""||value.Title==""||value.Head.Ref==""||value.Base.Ref==""||!programRepo(value.Head.Repo.FullName)||!programRepo(value.Base.Repo.FullName)||!programCommit(value.Head.SHA)||!programCommit(value.Base.SHA)||value.State!="open"&&value.State!="closed"{return errors.New("incomplete PR identity")}
	*p=EffectPullRequest(value)
	return nil
}

func (c Client) EffectPullRequests(ctx context.Context,repo string)([]EffectPullRequest,error){
	if !programRepo(repo){return nil,errors.New("invalid PR repository")}
	data,err:=c.gh(ctx,"api","--paginate","repos/"+repo+"/pulls?state=all&per_page=100")
	if err!=nil{return nil,err}
	return effectPages[EffectPullRequest](data)
}

func (c Client) EffectPullRequest(ctx context.Context,repo string,number int)(EffectPullRequest,error){
	var result EffectPullRequest
	if !programRepo(repo)||number<1{return result,errors.New("invalid PR target")}
	err:=c.getJSON(ctx,&result,"api",fmt.Sprintf("repos/%s/pulls/%d",repo,number))
	return result,err
}

// ProgramWriter has no model-supplied endpoint or command. Its caller must first
// obtain the programme actor's actual claim and credential capability.
type ProgramWriter struct {
	token string
	client *http.Client
}

func NewProgramWriter(token string)(ProgramWriter,error){
	if strings.TrimSpace(token)==""||strings.ContainsAny(token,"\r\n\x00"){return ProgramWriter{},errors.New("programme GitHub credential required")}
	transport:=http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy=nil
	transport.DisableKeepAlives=true
	return ProgramWriter{token:token,client:&http.Client{Transport:transport,Timeout:30*time.Second,CheckRedirect:func(*http.Request,[]*http.Request)error{return http.ErrUseLastResponse}}},nil
}

func (w ProgramWriter) post(ctx context.Context,path string,value any)([]byte,error){
	if w.client==nil||w.token==""{return nil,errors.New("programme writer unavailable")}
	data,err:=json.Marshal(value);if err!=nil{return nil,err}
	req,err:=http.NewRequestWithContext(ctx,http.MethodPost,"https://api.github.com/"+path,bytes.NewReader(data));if err!=nil{return nil,err}
	req.Header.Set("Authorization","Bearer "+w.token)
	req.Header.Set("Accept","application/vnd.github+json")
	req.Header.Set("Content-Type","application/json")
	req.Header.Set("X-GitHub-Api-Version","2022-11-28")
	// Do is called exactly once. A lost response stays uncertain; pure observers
	// recover the original action, rather than replaying this mutation.
	response,err:=w.client.Do(req);if err!=nil{return nil,err}
	defer response.Body.Close()
	const limit=8<<20
	body,err:=io.ReadAll(io.LimitReader(response.Body,limit+1));if err!=nil{return nil,err}
	if len(body)>limit{return nil,errors.New("incomplete programme mutation response")}
	if response.StatusCode<200||response.StatusCode>=300{return nil,fmt.Errorf("programme GitHub mutation response %d",response.StatusCode)}
	return body,nil
}

func (w ProgramWriter) CreateDraftPR(ctx context.Context,repo,branch,base,title,body string)(EffectPullRequest,error){
	var result EffectPullRequest
	if !programRepo(repo)||!programBranch(branch)||!programBranch(base)||strings.TrimSpace(title)==""{return result,errors.New("invalid draft PR request")}
	data,err:=w.post(ctx,"repos/"+repo+"/pulls",struct { Head string `json:"head"`; Base string `json:"base"`;Title string `json:"title"`;Body string `json:"body"`;Draft bool `json:"draft"` }{branch,base,title,body,true})
	if err!=nil{return result,err}
	err=json.Unmarshal(data,&result)
	return result,err
}

func (w ProgramWriter) ReadyPR(ctx context.Context,node string)error{
	if !programNode(node){return errors.New("invalid PR node")}
	const query=`mutation($id:ID!){markPullRequestReadyForReview(input:{pullRequestId:$id}){pullRequest{id isDraft}}}`
	data,err:=w.post(ctx,"graphql",struct{Query string `json:"query"`;Variables map[string]string `json:"variables"`}{query,map[string]string{"id":node}})
	if err!=nil{return err}
	var result struct{ Data struct{Ready struct{PR struct{ID string `json:"id"`;Draft *bool `json:"isDraft"`} `json:"pullRequest"`} `json:"markPullRequestReadyForReview"`} `json:"data"`;Errors []json.RawMessage `json:"errors"`}
	if err:=json.Unmarshal(data,&result);err!=nil{return err}
	if len(result.Errors)!=0||result.Data.Ready.PR.ID!=node||result.Data.Ready.PR.Draft==nil||*result.Data.Ready.PR.Draft{return errors.New("GitHub did not confirm ready PR")}
	return nil
}

func (w ProgramWriter) ReplyReviewThread(ctx context.Context,repo string,number int,firstComment int64,body string)error{
	if !programRepo(repo)||number<1||firstComment<1||strings.TrimSpace(body)==""{return errors.New("invalid review reply")}
	_,err:=w.post(ctx,"repos/"+repo+"/pulls/"+strconv.Itoa(number)+"/comments/"+strconv.FormatInt(firstComment,10)+"/replies",struct{Body string `json:"body"`}{body})
	return err
}

func (w ProgramWriter) CommentPR(ctx context.Context,repo string,number int,body string)error{
	if !programRepo(repo)||number<1||strings.TrimSpace(body)==""{return errors.New("invalid PR comment")}
	_,err:=w.post(ctx,fmt.Sprintf("repos/%s/issues/%d/comments",repo,number),struct{Body string `json:"body"`}{body})
	return err
}

func (w ProgramWriter) ResolveReviewThread(ctx context.Context,node string)error{
	if !programNode(node){return errors.New("invalid review thread")}
	const query=`mutation($id:ID!){resolveReviewThread(input:{threadId:$id}){thread{id isResolved}}}`
	data,err:=w.post(ctx,"graphql",struct{Query string `json:"query"`;Variables map[string]string `json:"variables"`}{query,map[string]string{"id":node}})
	if err!=nil{return err}
	var result struct{Data struct{Resolve struct{Thread struct{ID string `json:"id"`;Resolved bool `json:"isResolved"`} `json:"thread"`} `json:"resolveReviewThread"`} `json:"data"`;Errors []json.RawMessage `json:"errors"`}
	if err:=json.Unmarshal(data,&result);err!=nil{return err}
	if len(result.Errors)!=0||result.Data.Resolve.Thread.ID!=node||!result.Data.Resolve.Thread.Resolved{return errors.New("GitHub did not confirm thread resolution")}
	return nil
}

func programRepo(repo string)bool{
	parts:=strings.Split(repo,"/");if len(parts)!=2||parts[0]==""||parts[1]==""||parts[0]=="."||parts[0]==".."||parts[1]=="."||parts[1]==".."{return false}
	for _,c:=range repo{if !(c>='a'&&c<='z'||c>='A'&&c<='Z'||c>='0'&&c<='9'||strings.ContainsRune("/._-",c)){return false}}
	return true
}

func programBranch(branch string)bool{return branch!=""&&!strings.HasPrefix(branch,"-")&&!strings.ContainsAny(branch,"\r\n\x00:?*[\\ ~^")&&!strings.Contains(branch,"..")&&!strings.Contains(branch,"@{")&&!strings.HasSuffix(branch,"/")&&!strings.HasSuffix(branch,".")&&!strings.HasSuffix(branch,".lock")}
func programNode(node string)bool{return strings.TrimSpace(node)==node&&node!=""&&!strings.ContainsAny(node,"\r\n\x00")}
func programCommit(commit string)bool{if len(commit)!=40&&len(commit)!=64{return false};for _,c:=range commit{if !(c>='0'&&c<='9'||c>='a'&&c<='f'){return false}};return true}
