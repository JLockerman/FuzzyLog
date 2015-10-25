
use std::convert::AsRef;
use std::fmt::Write;
use std::io::Read;

use rusoto::regions::Region;
use rusoto::signature::SignedRequest;
use rusoto::credentials::AWSCredentials;

use rustc_serialize::{Encodable, Decodable, json};
use rustc_serialize::base64::{Config, ToBase64, FromBase64, CharacterSet, Newline};

use hyper::status::StatusCode;

use prelude::*;

#[derive(Debug)]
pub struct DynDBStore<S: AsRef<str>> {
    table_name: S,
    region: Region,
    credentials: AWSCredentials,
}

impl<S: AsRef<str>> DynDBStore<S> {
    pub fn new(table_name: S, region: Region, creds: AWSCredentials) -> Self {
        DynDBStore {
            table_name: table_name,
            region: region,
            credentials: creds,
        }
    }
}

impl<V, S> Store<V> for DynDBStore<S>
where Entry<V>: Encodable + Decodable, S: AsRef<str> {
    fn insert(&mut self, key: OrderIndex, val: Entry<V>) -> InsertResult {
        let payload = build_put_string(self.table_name.as_ref(), order_index_to_u64(key), val).into_bytes();
        trace!("{}", String::from_utf8_lossy(&*payload));
        let mut request = SignedRequest::new("POST", "dynamodb", &self.region, "/");
        request.set_content_type("application/x-amz-json-1.0");
        request.add_header("X-Amz-Target", "DynamoDB_20120810.PutItem");
        request.set_payload(Some(&*payload));
        //TODO let result = try!(..)
        let mut result = request.sign_and_execute(&self.credentials);
        match result.status {
            StatusCode::Ok => Ok(()),
            StatusCode::BadRequest => {
                let mut body = String::new();
                result.read_to_string(&mut body).unwrap();
                assert_eq!(body, r#"{"__type":"com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException","message":"The conditional request failed"}"#);
                Err(InsertErr::AlreadyWritten)
            }
            s => panic!("status code {}", s),
        }
    }

    fn get(&mut self, key: OrderIndex) -> GetResult<Entry<V>> {
        let payload = build_get_string(self.table_name.as_ref(), order_index_to_u64(key)).into_bytes();
        trace!("{} {}", line!(), String::from_utf8_lossy(&*payload));
        let mut request = SignedRequest::new("POST", "dynamodb", &self.region, "/");
        request.set_content_type("application/x-amz-json-1.0");
        request.add_header("X-Amz-Target", "DynamoDB_20120810.GetItem");
        request.set_payload(Some(&*payload));
        let mut result = request.sign_and_execute(&self.credentials);
        match result.status {
            StatusCode::Ok => {
                let mut body = String::new();
                result.read_to_string(&mut body).expect("could not read result");
                if body == "{}" {
                    return Err(GetErr::NoValue)
                }
                trace!("{}", body);
                let item: AWSItem = match json::decode(&*body) {
                    Ok(i) => i,
                    Err(a) => panic!("msg for {:?} {}\n decoding error {:?}", key, body, a),
                };
                let entry_vec = item.Item.Entry.B.from_base64().expect("not base 64");
                let entry_string = String::from_utf8(entry_vec).expect("not utf8");
                Ok(json::decode(&*entry_string).expect("not json"))
            }
            s => panic!("status code {}", s),
        }
    }
}

impl<S: AsRef<str> + Clone> Clone for DynDBStore<S> {

    fn clone(&self) -> Self {
        let &DynDBStore {
            ref table_name, ref region, ref credentials,
        } = self;
        let region = match region {
            &Region::UsEast1 => Region::UsEast1,
            &Region::UsWest1 => Region::UsWest1,
            &Region::UsWest2 => Region::UsWest2,
            &Region::EuWest1 => Region::EuWest1,
            &Region::EuCentral1 => Region::EuCentral1,
            &Region::ApSoutheast1 => Region::ApSoutheast1,
            &Region::ApNortheast1 => Region::ApNortheast1,
            &Region::ApSoutheast2 => Region::ApSoutheast2,
            &Region::SaEast1 => Region::SaEast1,
        };
        DynDBStore {
            table_name: table_name.clone(),
            region: region,
            credentials: credentials.clone(),
        }
    }
}

#[allow(non_snake_case)]
#[derive(Debug, RustcDecodable, RustcEncodable)]
struct AWSItem {
    Item: Item
}

#[allow(non_snake_case)]
#[derive(Debug, RustcDecodable, RustcEncodable)]
struct Item {
    Entry: AWSBin,
    OrderIndex: AWSNum,
}

#[allow(non_snake_case)]
#[derive(Debug, RustcDecodable, RustcEncodable)]
struct AWSBin {
    B: String,
}

#[allow(non_snake_case)]
#[derive(Debug, RustcDecodable, RustcEncodable)]
struct AWSNum {
    N: String,
}

fn build_get_string(table_name: &str, index: u64) -> String {
    let prefix = r#"{"TableName":""#;
    let mid = r#"","ConsistentRead":true,"Key":{"OrderIndex":{"N":""#;
    let suffix = r#""}}}"#; //TODO add projection entry
    let mut payload: String = prefix.into();
    payload.push_str(table_name);
    payload.push_str(mid);
    payload.write_fmt(format_args!("{}", index)).expect("number format error");
    payload.push_str(suffix);
    payload
}

fn build_put_string<V: Encodable>(table_name: &str, index: u64, value: V) -> String {
    let config = Config {
        char_set: CharacterSet::Standard,
        newline: Newline::LF,
        pad: true,
        line_length: None,
    };
    let prefix = r#"{"TableName":""#;
    //TODO consisten read
    let mid1 = r#"","ConditionExpression":"attribute_not_exists(OrderIndex)","Item":{"OrderIndex":{"N":""#;
    let mid2 = r#""},"Entry":{"B":""#;
    let suffix = r#""}}}"#;
    let mut payload: String = prefix.into();
    payload.push_str(table_name);
    payload.push_str(mid1);
    payload.write_fmt(format_args!("{}", index)).expect("number format error");
    payload.push_str(mid2);
    payload.push_str(&*json::encode(&value).unwrap().into_bytes().to_base64(config));
    payload.push_str(suffix);
    payload
}

#[allow(dead_code)]
fn build_delete_string(table_name: &str, index: u64) -> String {
    let prefix = r#"{"TableName":""#;
    let mid = r#"","Key":{"OrderIndex":{"N":""#;
    let suffix = r#""}}}"#;
    let mut payload: String = prefix.into();
    payload.push_str(table_name);
    payload.push_str(mid);
    payload.write_fmt(format_args!("{}", index)).expect("number format error");
    payload.push_str(suffix);
    payload
}

#[cfg(all(test, feature = "dynamodb_tests"))]
mod test {
    extern crate env_logger;

    use prelude::*;
    use local_store::{MapEntry};

    use super::*;

    use std::io::Read;

    use rusoto::regions::Region;
    use rusoto::signature::SignedRequest;
    use rusoto::credentials::{AWSCredentials, AWSCredentialsProvider};
    use rusoto::credentials::DefaultAWSCredentialsProviderChain;

    #[test]
    fn test_dynamo_get() {
        let _ = env_logger::init();
        let mut provider = DefaultAWSCredentialsProviderChain::new();
        let region = Region::UsEast1;
        let table = "FLTable";
        let creds = provider.get_credentials().ok().unwrap();
        delete_entry(&table, &region, creds, (0.into(), 1.into()));
        {
            let payload = super::build_put_string(table, 1, MapEntry(0, 32)).into_bytes();
            let mut request = SignedRequest::new("POST", "dynamodb", &region, "/");
            request.set_content_type("application/x-amz-json-1.0");
            request.add_header("X-Amz-Target", "DynamoDB_20120810.PutItem");
            request.set_payload(Some(&*payload));
            let result = request.sign_and_execute(creds);
            let status = result.status.to_u16();
            assert_eq!(status, 200);
        }
        {
            let payload = super::build_get_string(table, 1).into_bytes();
            //println!("payload:\n{}\n", String::from_utf8_lossy(&*payload));
            let mut request = SignedRequest::new("POST", "dynamodb", &region, "/");
            request.set_content_type("application/x-amz-json-1.0");
            request.add_header("X-Amz-Target", "DynamoDB_20120810.GetItem");
            //let payload = br#"{"TableName":"FLTable","Key":{"OrderIndex":{"N":"1"}},"ConsistentRead":true}"#;
            request.set_payload(Some(&*payload));
            let mut result = request.sign_and_execute(creds);
            let status = result.status.to_u16();
            let mut body = String::new();
            result.read_to_string(&mut body).unwrap();
            assert_eq!(status, 200);
            assert_eq!(body, "{\"Item\":{\"Entry\":{\"B\":\"eyJfZmllbGQwIjowLCJfZmllbGQxIjozMn0=\"},\"OrderIndex\":{\"N\":\"1\"}}}");
            //println!("status {} {}", status, body)
        }
    }

    #[test]
    fn test_dynamo_put() {
        let _ = env_logger::init();
        let mut provider = DefaultAWSCredentialsProviderChain::new();
        let region = Region::UsEast1;
        let table = "FLTable";
        let creds = provider.get_credentials().ok().unwrap();
        delete_entry(&table, &region, creds, (0.into(), 1.into()));
        let payload = super::build_put_string(table, 1, MapEntry(0, 32)).into_bytes();
        //println!("payload:\n{}\n", String::from_utf8_lossy(&*payload));
        let mut request = SignedRequest::new("POST", "dynamodb", &region, "/");
        request.set_content_type("application/x-amz-json-1.0");
        request.add_header("X-Amz-Target", "DynamoDB_20120810.PutItem");
        request.set_payload(Some(&*payload));
        let result = request.sign_and_execute(creds);
        let status = result.status.to_u16();
        assert_eq!(status, 200);
        let mut result = request.sign_and_execute(creds);
        let status = result.status.to_u16();
        let mut body = String::new();
        result.read_to_string(&mut body).unwrap();
        assert_eq!(status, 400);
        assert_eq!(body, r#"{"__type":"com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException","message":"The conditional request failed"}"#);
        //println!("status {} {}", status, body)
    }

    #[test]
    fn test_dynamo_store() {
        let _ = env_logger::init();
        let table = "FLTable";
        let region = Region::UsEast1;
        let creds =  DefaultAWSCredentialsProviderChain::new().get_credentials().ok().unwrap().clone();
        delete_entry(&table, &region, &creds, (0.into(), 8.into()));
        let mut store = DynDBStore {
            credentials: creds,
            region: Region::UsEast1,
            table_name: table,
        };
        let key = (0.into(), 8.into());
        let val = Entry::Data(MapEntry(9, 92), vec![]);
        let _ = store.insert(key, val.clone());
        let res = store.get(key).unwrap();
        assert_eq!(res, val);
    }

    fn delete_entry<S: AsRef<str>>(name: &S, region: &Region,
        credentials: &AWSCredentials, loc: (order, entry)) {
        let payload = super::build_delete_string(name.as_ref(), order_index_to_u64(loc)).into_bytes();
        let mut request = SignedRequest::new("POST", "dynamodb", &region, "/");
        request.set_content_type("application/x-amz-json-1.0");
        request.add_header("X-Amz-Target", "DynamoDB_20120810.DeleteItem");
        request.set_payload(Some(&*payload));
        let result = request.sign_and_execute(&credentials);
        trace!("{:?}", result);
    }

    fn new_dyndb_store(to_delete: Vec<OrderIndex>) -> DynDBStore<&'static str> {
        let store = DynDBStore::new("FLTable", Region::UsEast1,
            DefaultAWSCredentialsProviderChain::new()
                .get_credentials().ok().unwrap().clone());
        for i in to_delete {
            delete_entry(&store.table_name, &store.region,
                &store.credentials, i);
        }
        store
    }

    general_tests!(super::new_dyndb_store);
}
