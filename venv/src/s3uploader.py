from io import StringIO
from io import BytesIO
import os
import boto3


class MultiPartUploader:
    def __init__(self, bucket_name, obj_name, buf_size=5 * 1024 * 1024):
        self.bucket_name = bucket_name
        self.obj_name = obj_name
        self.buf_size = buf_size

        self.buffer = StringIO()
        self.client = boto3.client('s3',
                                   aws_access_key_id=" ",
                                   aws_secret_access_key=" ",
                                   aws_session_token=" ")
        response = self.client.create_multipart_upload(Bucket=self.bucket_name,
                                                       Key=self.obj_name)
        self.uploadId = response["UploadId"]
        self.part = 0
        self.parts = []

    def len(self):
        self.buffer.seek(0, os.SEEK_END)
        return self.buffer.tell()

    def abort(self):
        self.client.abort_multipart_upload(Bucket=self.bucket_name,
                                           Key=self.obj_name,
                                           UploadId=self.uploadId)
        self.buffer = StringIO()
        self.part = 0
        self.parts = []
        self.uploadId = None

    def write(self, line: str):
        self.buffer.write(line)
        if self.len() > self.buf_size:
            self.flush()

    def flush(self):
        if self.len() > 0:
            self.part = self.part + 1
            buffer_to_upload = BytesIO(self.buffer.getvalue().encode())
            uploaded_part = self.client.upload_part(Body=buffer_to_upload,
                                                    Bucket=self.bucket_name,
                                                    Key=self.obj_name,
                                                    PartNumber=self.part,
                                                    UploadId=self.uploadId)

            part_info = {'PartNumber': self.part, 'ETag': uploaded_part['ETag']}
            self.parts.append(part_info)

            self.buffer.close()
            self.buffer = StringIO()

    def close(self):
        self.flush()
        if len(self.parts) > 0:
            self.client.complete_multipart_upload(Bucket=self.bucket_name,
                                                  Key=self.obj_name,
                                                  MultipartUpload={
                                                      'Parts': self.parts
                                                  },
                                                  UploadId=self.uploadId
                                                  )

        self.buffer = StringIO()
        self.part = 0
        self.uploadId = None
