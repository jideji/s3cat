package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var uriRegex = regexp.MustCompile("s3://([^/]*)(.*)")

const bucketGroup = 1
const keyGroup = 2

func main() {
	profile := flag.String("profile", "default", os.ExpandEnv("aws account profile"))
	region := flag.String("region", "eu-west-1", "aws site region")
	flag.Parse()

	if len(flag.Args()) == 0 {
		flag.Usage()
		os.Exit(1)
	}

	err := execute(credentials.NewSharedCredentials("", *profile), *region, os.Stdout, flag.Args()...)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
	os.Stdout.Sync()
}

func execute(credentials *credentials.Credentials, region string, w io.Writer, s3Uris ...string) error {
	session := session.New(aws.NewConfig().WithCredentials(credentials).WithRegion(region))
	s3Client := s3.New(session)

	for _, s3uri := range s3Uris {
		err := catS3Uri(s3Client, s3uri, w)
		if err != nil {
			return err
		}
	}
	return nil
}

func catS3Uri(s3Client *s3.S3, s3uri string, w io.Writer) error {
	groups := uriRegex.FindStringSubmatch(s3uri)
	if len(groups) == 0 {
		return fmt.Errorf("Invalid s3 uri: %s", s3uri)
	}
	bucket := groups[bucketGroup]
	key := groups[keyGroup]

	err := catS3BucketKey(s3Client, bucket, key, w)
	if err != nil {
		return err
	}
	return nil
}

func catS3BucketKey(s3Client *s3.S3, bucket, key string, w io.Writer) error {
	goi := s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	goo, err := s3Client.GetObject(&goi)

	if err != nil {
		return err
	}

	err = cat(goo.Body, w)
	if err != nil {
		return err
	}
	return nil
}

func cat(body io.ReadCloser, w io.Writer) error {
	br := bufio.NewReader(body)
	defer body.Close()
	buf := make([]byte, 4096)
	for {
		length, err := br.Read(buf)
		if length > 0 {
			w.Write(buf[:length])
		}
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
	}
}
