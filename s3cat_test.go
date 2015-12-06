package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func TestRunStuffs(t *testing.T) {
	conf := config(t)

	data := bytes.NewReader([]byte("the data"))

	creds := credentials.NewStaticCredentials(conf.Id, conf.Secret, "")
	session := session.New(aws.NewConfig().WithCredentials(creds).WithRegion(conf.Region))
	s3Client := s3.New(session)

	cbi := s3.CreateBucketInput{
		Bucket: aws.String(conf.S3bucket),
	}
	_, err := s3Client.CreateBucket(&cbi)
	poi := s3.PutObjectInput{
		Bucket: aws.String(conf.S3bucket),
		Key:    aws.String("filename"),
		Body:   data,
	}
	_, err = s3Client.PutObject(&poi)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer

	execute(creds, conf.Region, &buf, fmt.Sprintf("s3://%s/filename", conf.S3bucket))

	output := buf.String()
	if output != "the data" {
		t.Errorf("Got '%s' expected '%s'", "the data", output)
	}
}

func config(t *testing.T) *configStruct {
	data, err := ioutil.ReadFile("test.conf")

	if err != nil {
		data, err = json.Marshal(configStruct{})
		t.Fatal("Please create a test.conf file of the following format:\n" + string(data))
	}

	var conf configStruct
	if err := json.Unmarshal(data, &conf); err != nil {
		panic(err)
	}

	return &conf
}

type configStruct struct {
	Id       string `json:"access-key-id"`
	Secret   string `json:"secret-access-key"`
	Region   string `json:"region"`
	S3bucket string `json:"s3-bucket"`
}
