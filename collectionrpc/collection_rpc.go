// Code generated by goctl. DO NOT EDIT.
// Source: collection.proto

package collectionrpc

import (
	"context"

	"github.com/xh-polaris/meowchat-collection-rpc/pb"

	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
)

type (
	Cat             = pb.Cat
	CreateCatReq    = pb.CreateCatReq
	CreateCatResp   = pb.CreateCatResp
	DeleteCatReq    = pb.DeleteCatReq
	DeleteCatResp   = pb.DeleteCatResp
	ListCatReq      = pb.ListCatReq
	ListCatResp     = pb.ListCatResp
	RetrieveCatReq  = pb.RetrieveCatReq
	RetrieveCatResp = pb.RetrieveCatResp
	SearchCatReq    = pb.SearchCatReq
	SearchCatResp   = pb.SearchCatResp
	UpdateCatReq    = pb.UpdateCatReq
	UpdateCatResp   = pb.UpdateCatResp

	CollectionRpc interface {
		SearchCat(ctx context.Context, in *SearchCatReq, opts ...grpc.CallOption) (*SearchCatResp, error)
		ListCat(ctx context.Context, in *ListCatReq, opts ...grpc.CallOption) (*ListCatResp, error)
		RetrieveCat(ctx context.Context, in *RetrieveCatReq, opts ...grpc.CallOption) (*RetrieveCatResp, error)
		CreateCat(ctx context.Context, in *CreateCatReq, opts ...grpc.CallOption) (*CreateCatResp, error)
		UpdateCat(ctx context.Context, in *UpdateCatReq, opts ...grpc.CallOption) (*UpdateCatResp, error)
		DeleteCat(ctx context.Context, in *DeleteCatReq, opts ...grpc.CallOption) (*DeleteCatResp, error)
	}

	defaultCollectionRpc struct {
		cli zrpc.Client
	}
)

func NewCollectionRpc(cli zrpc.Client) CollectionRpc {
	return &defaultCollectionRpc{
		cli: cli,
	}
}

func (m *defaultCollectionRpc) SearchCat(ctx context.Context, in *SearchCatReq, opts ...grpc.CallOption) (*SearchCatResp, error) {
	client := pb.NewCollectionRpcClient(m.cli.Conn())
	return client.SearchCat(ctx, in, opts...)
}

func (m *defaultCollectionRpc) ListCat(ctx context.Context, in *ListCatReq, opts ...grpc.CallOption) (*ListCatResp, error) {
	client := pb.NewCollectionRpcClient(m.cli.Conn())
	return client.ListCat(ctx, in, opts...)
}

func (m *defaultCollectionRpc) RetrieveCat(ctx context.Context, in *RetrieveCatReq, opts ...grpc.CallOption) (*RetrieveCatResp, error) {
	client := pb.NewCollectionRpcClient(m.cli.Conn())
	return client.RetrieveCat(ctx, in, opts...)
}

func (m *defaultCollectionRpc) CreateCat(ctx context.Context, in *CreateCatReq, opts ...grpc.CallOption) (*CreateCatResp, error) {
	client := pb.NewCollectionRpcClient(m.cli.Conn())
	return client.CreateCat(ctx, in, opts...)
}

func (m *defaultCollectionRpc) UpdateCat(ctx context.Context, in *UpdateCatReq, opts ...grpc.CallOption) (*UpdateCatResp, error) {
	client := pb.NewCollectionRpcClient(m.cli.Conn())
	return client.UpdateCat(ctx, in, opts...)
}

func (m *defaultCollectionRpc) DeleteCat(ctx context.Context, in *DeleteCatReq, opts ...grpc.CallOption) (*DeleteCatResp, error) {
	client := pb.NewCollectionRpcClient(m.cli.Conn())
	return client.DeleteCat(ctx, in, opts...)
}
