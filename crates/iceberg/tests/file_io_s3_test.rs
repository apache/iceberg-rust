use futures::{AsyncReadExt, AsyncWriteExt};
use iceberg::io::{
    FileIO, FileIOBuilder, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY,
};
use iceberg_test_utils::docker::DockerCompose;
use port_scanner::scan_port_addr;
use tokio::time::sleep;

struct MinIOFixture {
    _docker_compose: DockerCompose,
    file_io: FileIO,
}
impl MinIOFixture {
    async fn new(project_name: impl ToString) -> Self {
        // Start the Docker container for the test fixture
        let docker = DockerCompose::new(
            project_name.to_string(),
            format!("{}/testdata/file_io_s3", env!("CARGO_MANIFEST_DIR")),
        );
        docker.run();
        let container_ip = docker.get_container_ip("minio");
        let read_port = format!("{}:{}", container_ip, 9000);
        loop {
            if !scan_port_addr(&read_port) {
                log::info!("Waiting for 1s rest catalog to ready...");
                sleep(std::time::Duration::from_millis(1000)).await;
            } else {
                break;
            }
        }
        MinIOFixture {
            _docker_compose: docker,
            file_io: FileIOBuilder::new("s3")
                .with_props(vec![
                    (S3_ENDPOINT, format!("http://{}", read_port)),
                    (S3_ACCESS_KEY_ID, "admin".to_string()),
                    (S3_SECRET_ACCESS_KEY, "password".to_string()),
                    (S3_REGION, "us-east-1".to_string()),
                ])
                .build()
                .unwrap(),
        }
    }
}

#[tokio::test]
async fn test_file_io_s3_is_exist() {
    let fixture = MinIOFixture::new("test_file_io_s3_is_exist").await;
    assert!(!fixture.file_io.is_exist("s3://bucket2/any").await.unwrap());
    assert!(fixture.file_io.is_exist("s3://bucket1/").await.unwrap());
}

#[tokio::test]
async fn test_file_io_s3_output() {
    // Start the Docker container for the test fixture
    let fixture = MinIOFixture::new("test_file_io_s3_output").await;
    assert!(!fixture
        .file_io
        .is_exist("s3://bucket1/test_output")
        .await
        .unwrap());
    let output_file = fixture
        .file_io
        .new_output("s3://bucket1/test_output")
        .unwrap();
    {
        let mut writer = output_file.writer().await.unwrap();
        writer.write_all("123".as_bytes()).await.unwrap();
        writer.close().await.unwrap();
    }
    assert!(fixture
        .file_io
        .is_exist("s3://bucket1/test_output")
        .await
        .unwrap());
}

#[tokio::test]
async fn test_file_io_s3_input() {
    let fixture = MinIOFixture::new("test_file_io_s3_input").await;
    let output_file = fixture
        .file_io
        .new_output("s3://bucket1/test_input")
        .unwrap();
    {
        let mut writer = output_file.writer().await.unwrap();
        writer.write_all("test_input".as_bytes()).await.unwrap();
        writer.close().await.unwrap();
    }
    let input_file = fixture
        .file_io
        .new_input("s3://bucket1/test_input")
        .unwrap();
    {
        let mut reader = input_file.reader().await.unwrap();
        let mut buffer = vec![];
        reader.read_to_end(&mut buffer).await.unwrap();
        assert_eq!(buffer, "test_input".as_bytes());
    }
}
