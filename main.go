package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/xataio/xata-go/xata"
)

func build_pagination(offset int, size int) *xata.PageConfig {
	return &xata.PageConfig{
		Offset: &offset,
		Size:   &size,
	}
}

type DBName string

const (
	debug            = true
	throttle         = 100
	ZoeBeauty DBName = "zoes-beauty"
)

func (d DBName) String() *string {
	return (*string)(&d)
}

type Summary struct {
	TotalRows int `json:"total_rows"`
}

type MetaDataSummary struct {
	Summaries []Summary `json:"summaries"`
}

func build_branch_req(dbname DBName, branchName string) xata.BranchRequestOptional {
	return xata.BranchRequestOptional{
		DatabaseName: dbname.String(),
		BranchName:   &branchName,
	}
}

type PaginationConfig struct {
	Offset    int
	Size      int
	TotalRows Option[int]
	NumPages  Option[int]
}

type MessageResponse struct {
	Records []MessageRecord `json:"records"`
}

type MessageRecord struct {
	ID           string `json:"id"`
	CreatedAt    string `json:"created_at"`
	UpdatedAt    string `json:"updated_at"`
	Body         string `json:"body"`
	SenderName   string `json:"sender_name"`
	ReceiverName string `json:"receiver_name"`
}

type XataClient struct {
	branchReq    *xata.BranchRequestOptional
	searchClient *xata.SearchAndFilterClient
	ctx          context.Context
	config       PaginationConfig
}

func (x *XataClient) New(branchReq xata.BranchRequestOptional, config PaginationConfig) *XataClient {
	if searchClient, err := xata.NewSearchAndFilterClient(xata.ClientOption(
		xata.WithBaseURL("https://Jules-Workspace-b855j9.us-east-1.xata.sh"),
	)); err == nil {
		return &XataClient{
			branchReq:    &branchReq,
			searchClient: &searchClient,
			ctx:          context.Background(),
			config:       config,
		}
	} else {
		fmt.Println("Error creating client: ", err)
		return nil
	}
}

func (x *XataClient) IsInitialized() bool {
	println("searchClient", x.searchClient != nil)
	println("branchReq", x.branchReq != nil)
	println("ctx", x.ctx != nil)
	println("config", x.config != (PaginationConfig{}))
	return x.searchClient != nil && x.branchReq != nil && x.ctx != nil && x.config != (PaginationConfig{})
}

func (x *XataClient) fetch_by_page(page int) Option[MessageResponse] {
	if !x.IsInitialized() {
		fmt.Println("Client not initialized")
		return None[MessageResponse]()
	}

	println("Fetching page: ", page)
	println("with branchReq", x.branchReq)
	println("with config", fmt.Sprintf("%v", x.config))

	resp, err := (*x.searchClient).Query(x.ctx, xata.QueryTableRequest{
		TableName:             "messages",
		BranchRequestOptional: *x.branchReq,
		Payload: xata.QueryTableRequestPayload{
			Columns: []string{},
			Page:    build_pagination(x.config.Offset, x.config.Size),
		},
	})

	println(resp)

	if err != nil {
		fmt.Println("Error querying data: ", err)
	} else {
		var data MessageResponse
		respBytes, _ := json.Marshal(resp)
		json.Unmarshal(respBytes, &data)

		return Some[MessageResponse](data)
	}
	return None[MessageResponse]()
}

func calc_num_pages(totalRows int, size int) int {
	return totalRows / size
}

func main() {
	// workspaceCli, err := xata.NewWorkspacesClient()
	// if err != nil {
	// 	log.Fatal(err)
	// }

	println("Starting, calling client")

	var builder XataClient
	var client = builder.New(
		build_branch_req(ZoeBeauty, "main"),
		PaginationConfig{
			Offset:    0,
			Size:      10,
			TotalRows: None[int](),
			NumPages:  None[int](),
		},
	)

	if client == nil {
		fmt.Println("Client errorrrrrrr aaaaaaa")
		return
	}

	// searchClient := *xataClient.searchClient

	res := client.fetch_by_page(1)

	if res.IsEmpty() {
		fmt.Println("No data found")
	} else {
		fmt.Println("Data found")
		fmt.Println(res.Get())
	}

	// // parse the metadata from json to struct
	// var metadata MetaDataSummary
	// if e2 := json.Unmarshal(metadataBytes, &metadata); e2 != nil {
	// 	fmt.Println("Error unmarshalling metadata: ", e2)
	// 	return
	// }

	// totalRows := metadata.Summaries[0].TotalRows
	// numPages := calc_num_pages(totalRows, size)

	// if debug {
	// 	fmt.Println("Total rows: ", totalRows)
	// 	fmt.Println("Number of pages: ", numPages)
	// }

	// if queryErr != nil {
	// 	fmt.Println(queryErr)
	// } else {
	// 	json1, e1 := json.MarshalIndent(*data, "", "  ")

	// 	if e1 != nil {
	// 		fmt.Println("Error marshalling data: ", err)
	// 		return
	// 	}

	// 	fmt.Println(string(json1))
	// }

	// // wait for user input
	// fmt.Scanln()
}
