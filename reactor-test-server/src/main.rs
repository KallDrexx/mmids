use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Result, Server, StatusCode};
use serde::Deserialize;
use tokio::fs::File;
use tokio_util::codec::{BytesCodec, FramedRead};

const DIRECTORY: &'static str = "workflows";

#[derive(Deserialize)]
struct RequestContent {
    stream_name: String,
}

#[tokio::main]
async fn main() {
    let addr = "127.0.0.1:9055".parse().unwrap();
    let make_service =
        make_service_fn(|_| async { Ok::<_, hyper::Error>(service_fn(get_response)) });

    let server = Server::bind(&addr).serve(make_service);

    println!("Listening on http://{}", addr);

    if let Err(e) = server.await {
        eprintln!("server error: {}", e);
    }
}

async fn get_response(req: Request<Body>) -> Result<Response<Body>> {
    match (req.method(), req.uri().path()) {
        (&Method::POST, "/") => Ok(not_found()),

        (&Method::POST, path) => {
            let sub_directory = (&path[1..]).to_string();
            let whole_body = hyper::body::to_bytes(req.into_body()).await?;
            let content: RequestContent = match serde_json::from_slice(&whole_body) {
                Ok(content) => content,
                Err(error) => {
                    println!("Error parsing json from body: {:?}", error);

                    return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(format!("Error parsing json from body: {:?}", error).into())
                        .unwrap());
                }
            };

            println!(
                "Request came in for {}/'{}'",
                sub_directory, content.stream_name
            );

            let path = format!(
                "{}/{}/{}.mmids",
                DIRECTORY, sub_directory, content.stream_name
            );

            if let Ok(file) = File::open(&path).await {
                let stream = FramedRead::new(file, BytesCodec::new());
                let body = Body::wrap_stream(stream);
                return Ok(Response::new(body));
            }

            println!("File '{}' not found", path);

            Ok(not_found())
        }

        _ => Ok(not_found()),
    }
}

fn not_found() -> Response<Body> {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body("not found".into())
        .unwrap()
}
