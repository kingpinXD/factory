package gh

import "context"

const mergeQueueQuery = `query($owner:String!,$name:String!,$branch:String!){repository(owner:$owner,name:$name){mergeQueue(branch:$branch){id}}}`

// MergeMethodFor returns how pull requests into branch merge: through its
// merge queue when it has one, else squashed at once.
func (c Client) MergeMethodFor(ctx context.Context, repo, branch string) (MergeMethod, error) {
	var data struct {
		Repository struct {
			MergeQueue *struct{ ID string }
		}
	}
	if err := c.graphql(ctx, &data, mergeQueueQuery, append(repoVars(repo), "-f", "branch="+branch)...); err != nil {
		return "", err
	}
	if data.Repository.MergeQueue != nil {
		return MergeQueue, nil
	}
	return MergeSquash, nil
}
