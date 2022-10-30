package main

import (
	"log"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gin-gonic/gin"
)

var svc *s3.S3

func init() {
	awsConfig := &aws.Config{
		Credentials:      credentials.NewStaticCredentials("<your-ak>", "<your-sk>", ""),
		Endpoint:         aws.String("<your-endpoint>"),
		Region:           aws.String("bj"),
		S3ForcePathStyle: aws.Bool(true),
		DisableSSL:       aws.Bool(true),
	}
	sess, err := session.NewSession(awsConfig)
	if err != nil {
		panic(err)
	}
	svc = s3.New(sess)
}

func main() {
	engine := gin.Default()
	group := engine.Group("/multipartUpload")
	group.POST("", create)
	group.GET("", list)
	group.PUT("", complete)
	group.DELETE("", abort)
	log.Fatal(engine.Run(":8080"))
}

func create(c *gin.Context) {
	var req CreateMultipartUploadRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, err)
		return
	}

	output, err := svc.CreateMultipartUploadWithContext(c, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(req.Bucket),
		Key:    aws.String(req.Key),
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, err)
		return
	}
	uploadID := output.UploadId

	resp := &CreateMultipartUploadResponse{
		UploadID: *uploadID,
		Parts:    make([]*Part, req.PartNumbers),
	}
	for i := int64(1); i <= req.PartNumbers; i++ {
		request, _ := svc.UploadPartRequest(&s3.UploadPartInput{
			Bucket:     aws.String(req.Bucket),
			Key:        aws.String(req.Key),
			PartNumber: aws.Int64(i),
			UploadId:   uploadID,
		})

		u, err := request.Presign(time.Hour * 24)
		if err != nil {
			c.JSON(http.StatusInternalServerError, err)
			return
		}

		resp.Parts[i-1] = &Part{
			PartNumber: i,
			PresignURL: &u,
		}
	}

	c.JSON(http.StatusOK, resp)
}

func list(c *gin.Context) {
	var req ListPartsRequest
	if err := c.ShouldBindQuery(&req); err != nil {
		c.JSON(http.StatusBadRequest, err)
		return
	}

	output, err := svc.ListPartsWithContext(c, &s3.ListPartsInput{
		Bucket:   aws.String(req.Bucket),
		Key:      aws.String(req.Key),
		UploadId: aws.String(req.UploadID),
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, err)
		return
	}

	resp := &ListPartsResponse{Parts: make([]*Part, len(output.Parts))}
	for i, part := range output.Parts {
		resp.Parts[i] = &Part{
			PartNumber: *part.PartNumber,
			ETag:       part.ETag,
			Size:       part.Size,
		}
	}
	c.JSON(http.StatusOK, resp)
}

func complete(c *gin.Context) {
	var req CompleteMultipartUploadRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, err)
		return
	}

	parts := make([]*s3.CompletedPart, len(req.Parts))
	for i, part := range req.Parts {
		parts[i] = &s3.CompletedPart{
			ETag:       part.ETag,
			PartNumber: aws.Int64(part.PartNumber),
		}
	}
	_, err := svc.CompleteMultipartUploadWithContext(c, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(req.Bucket),
		Key:      aws.String(req.Key),
		UploadId: aws.String(req.UploadID),
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: parts,
		},
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, err)
		return
	}

	c.JSON(http.StatusOK, nil)
}

func abort(c *gin.Context) {
	var req AbortMultiUploadRequest
	if err := c.ShouldBindQuery(&req); err != nil {
		c.JSON(http.StatusBadRequest, err)
		return
	}

	_, err := svc.AbortMultipartUploadWithContext(c, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(req.Bucket),
		Key:      aws.String(req.Key),
		UploadId: aws.String(req.UploadID),
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, err)
		return
	}

	c.JSON(http.StatusOK, nil)
}

type CreateMultipartUploadRequest struct {
	Bucket      string `json:"bucket" binding:"required"`
	Key         string `json:"key" binding:"required"`
	PartNumbers int64  `json:"partNumbers" binding:"required"`
}

type CreateMultipartUploadResponse struct {
	UploadID string  `json:"uploadID"`
	Parts    []*Part `json:"parts"`
}

type Part struct {
	PartNumber int64   `json:"partNumber" binding:"required"`
	ETag       *string `json:"eTag" binding:"required"`
	PresignURL *string `json:"presignURL"`
	Size       *int64  `json:"size"`
}

type ListPartsRequest struct {
	Bucket   string `form:"bucket" binding:"required"`
	Key      string `form:"key" binding:"required"`
	UploadID string `form:"uploadID" binding:"required"`
}

type ListPartsResponse struct {
	Parts []*Part `json:"parts"`
}

type CompleteMultipartUploadRequest struct {
	Bucket   string  `json:"bucket" binding:"required"`
	Key      string  `json:"key" binding:"required"`
	UploadID string  `json:"uploadID" binding:"required"`
	Parts    []*Part `json:"parts" binding:"required"`
}

type AbortMultiUploadRequest struct {
	Bucket   string `form:"bucket" binding:"required"`
	Key      string `form:"key" binding:"required"`
	UploadID string `form:"uploadID" binding:"required"`
}
