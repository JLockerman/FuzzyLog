
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

    use super::*;
    use local_store::{Map, MapEntry};

    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::io::Read;
    use std::rc::Rc;
    use std::sync::{Arc, Mutex};
    use std::thread;

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

    //Generic store tests

    fn new_dyndb_store() -> DynDBStore<&'static str> {
        let mut store = DynDBStore::new("FLTable", Region::UsEast1,
            DefaultAWSCredentialsProviderChain::new()
                .get_credentials().ok().unwrap().clone());
        clear_columns(&mut store);
        store
    }

    fn clear_columns<S: AsRef<str>>(store: &mut DynDBStore<S>) {
        for i in 0..50 {
            delete_entry(&store.table_name, &store.region,
                &store.credentials, (0.into(), i.into()));
            delete_entry(&store.table_name, &store.region,
                &store.credentials, (1.into(), i.into()));
            delete_entry(&store.table_name, &store.region,
                    &store.credentials, (2.into(), i.into()));
            delete_entry(&store.table_name, &store.region,
                &store.credentials, (3.into(), i.into()));
        }
    }

    #[test]
    fn test_get_none() {
        let _ = env_logger::init();
        let mut store = new_dyndb_store();
        let r = <Store<MapEntry<i32, i32>>>::get(&mut store, (0.into(), 0.into()));
        assert_eq!(r, Err(GetErr::NoValue))
    }

    #[test]
    fn test_threaded() {
        let _ = env_logger::init();
        let store = new_dyndb_store();
        let s = store.clone();
        let s1 = store.clone();
        let horizon = Arc::new(Mutex::new(HashMap::new()));
        let h = horizon.clone();
        let h1 = horizon.clone();
        let map0 = Rc::new(RefCell::new(HashMap::new()));
        let map1 = Rc::new(RefCell::new(HashMap::new()));
        let re0 = map0.clone();
        let re1 = map1.clone();
        let mut upcalls: HashMap<_, Box<Fn(_) -> bool>> = HashMap::new();
        upcalls.insert(0.into(), Box::new(move |MapEntry(k, v)| {
            re0.borrow_mut().insert(k, v);
            true
        }));
        upcalls.insert(1.into(), Box::new(move |MapEntry(k, v)| {
            re1.borrow_mut().insert(k, v);
            true
        }));
        let join = thread::spawn(move || {
            let mut log = FuzzyLog::new(s, h, HashMap::new());
            let mut last_index = (0.into(), 0.into());
            for i in 0..10 {
                last_index = log.append(0.into(), MapEntry(i * 2, i * 2), vec![]);
            }
            log.append(1.into(), MapEntry(5, 17), vec![last_index]);
        });
        let join1 = thread::spawn(|| {
            let mut log = FuzzyLog::new(s1, h1, HashMap::new());
            let mut last_index = (0.into(), 0.into());
            for i in 0..10 {
                last_index = log.append(0.into(), MapEntry(i * 2 + 1, i * 2 + 1), vec![]);
            }
            log.append(1.into(), MapEntry(9, 20), vec![last_index]);
        });
        join1.join().unwrap();
        join.join().unwrap();
        let mut log = FuzzyLog::new(store, horizon, upcalls);
        log.play_foward(1.into());

        let cannonical_map0 = {
            let mut map = HashMap::new();
            for i in 0..20 {
                map.insert(i, i);
            }
            map
        };
        let cannonical_map1 = {
            let mut map = HashMap::new();
            map.insert(5, 17);
            map.insert(9, 20);
            map
        };
        assert_eq!(*map1.borrow(), cannonical_map1);
        assert_eq!(*map0.borrow(), cannonical_map0);
    }

    #[test]
    fn test_1_column() {
        let _ = env_logger::init();
        let store = new_dyndb_store();
        let horizon = HashMap::new();
        let mut map = Map::new(store, horizon, 2.into());
        map.put(0, 1);
        map.put(1, 17);
        map.put(32, 5);
        assert_eq!(map.get(1), Some(17));
        assert_eq!(*map.local_view.borrow(), [(0,1), (1,17), (32,5)].into_iter().cloned().collect());
        assert_eq!(map.get(0), Some(1));
        assert_eq!(map.get(32), Some(5));
    }

    #[test]
    fn test_1_column_ni() {
        let _ = env_logger::init();
        let store = new_dyndb_store();
        let horizon = HashMap::new();
        let map = Rc::new(RefCell::new(HashMap::new()));
        let mut upcalls: HashMap<_, Box<Fn(_) -> bool>> = HashMap::new();
        let re = map.clone();
        upcalls.insert(0.into(), Box::new(move |MapEntry(k, v)| {
            re.borrow_mut().insert(k, v);
            true
        }));
        upcalls.insert(1.into(), Box::new(|_| false));
        let mut log = FuzzyLog::new(store, horizon, upcalls);
        log.append(0.into(), MapEntry(0, 1), vec![]);
        log.append(0.into(), MapEntry(1, 17), vec![]);
        let last_index = log.append(0.into(), MapEntry(32, 5), vec![]);
        log.append(1.into(), MapEntry(0, 0), vec![last_index]);
        log.play_foward(0.into());
        assert_eq!(*map.borrow(), [(0,1), (1,17), (32,5)].into_iter().cloned().collect());
    }

    #[test]
    fn test_deps() {
        let _ = env_logger::init();
        let store = new_dyndb_store();
        let horizon = HashMap::new();
        let map = Rc::new(RefCell::new(HashMap::new()));
        let mut upcalls: HashMap<_, Box<Fn(_) -> bool>> = HashMap::new();
        let re = map.clone();
        upcalls.insert(0.into(), Box::new(move |MapEntry(k, v)| {
            re.borrow_mut().insert(k, v);
            true
        }));
        upcalls.insert(1.into(), Box::new(|_| false));
        let mut log = FuzzyLog::new(store, horizon, upcalls);
        log.append(0.into(), MapEntry(0, 1), vec![]);
        log.append(0.into(), MapEntry(1, 17), vec![]);
        let last_index = log.append(0.into(), MapEntry(32, 5), vec![]);
        log.append(1.into(), MapEntry(0, 0), vec![last_index]);
        log.play_foward(1.into());
        assert_eq!(*map.borrow(), [(0,1), (1,17), (32,5)].into_iter().cloned().collect());
    }

    #[test]
    fn test_transaction_1() {
        let _ = env_logger::init();
        let store = new_dyndb_store();
        let horizon = HashMap::new();
        let map0 = Rc::new(RefCell::new(HashMap::new()));
        let map1 = Rc::new(RefCell::new(HashMap::new()));
        let map01 = Rc::new(RefCell::new(HashMap::new()));
        let mut upcalls: HashMap<_, Box<Fn(_) -> bool>> = HashMap::new();
        let re0 = map0.clone();
        let re1 = map1.clone();
        let re01 = map01.clone();
        upcalls.insert(0.into(), Box::new(move |MapEntry(k, v)| {
            re0.borrow_mut().insert(k, v);
            re01.borrow_mut().insert(k, v);
            true
        }));
        let re01 = map01.clone();
        upcalls.insert(1.into(), Box::new(move |MapEntry(k, v)| {
            re1.borrow_mut().insert(k, v);
            re01.borrow_mut().insert(k, v);
            true
        }));
        let mut log = FuzzyLog::new(store, horizon, upcalls);
        let mutations = vec![(0.into(), MapEntry(0, 1)), (1.into(), MapEntry(4.into(), 17)), (3.into(), MapEntry(22, 9))];
        {
            let transaction = log.start_transaction(mutations, Vec::new());
            transaction.commit();
        }

        log.play_foward(0.into());
        assert_eq!(*map0.borrow(), collect![0 => 1]);
        assert_eq!(*map1.borrow(), collect![4 => 17]);
        assert_eq!(*map01.borrow(), collect![0 => 1, 4 => 17]);
        log.play_foward(1.into());
        assert_eq!(*map0.borrow(), collect![0 => 1]);
        assert_eq!(*map1.borrow(), collect![4 => 17]);
        assert_eq!(*map01.borrow(), collect![0 => 1, 4 => 17]);
    }

    #[test]
    fn test_threaded_transaction() {
        let _ = env_logger::init();
        let store = new_dyndb_store();
        let s = store.clone();
        let s1 = store.clone();
        let horizon = Arc::new(Mutex::new(HashMap::new()));
        let h = horizon.clone();
        let h1 = horizon.clone();
        let map0 = Rc::new(RefCell::new(HashMap::new()));
        let map1 = Rc::new(RefCell::new(HashMap::new()));
        let re0 = map0.clone();
        let re1 = map1.clone();
        let mut upcalls: HashMap<_, Box<Fn(_) -> bool>> = HashMap::new();
        upcalls.insert(0.into(), Box::new(move |MapEntry(k, v)| {
            re0.borrow_mut().insert(k, v);
            true
        }));
        upcalls.insert(1.into(), Box::new(move |MapEntry(k, v)| {
            re1.borrow_mut().insert(k, v);
            true
        }));
        let join = thread::spawn(move || {
            let mut log = FuzzyLog::new(s, h, HashMap::new());
            for i in 0..10 {
                let change = vec![(0.into(), MapEntry(i * 2, i * 2)), (1.into(), MapEntry(i * 2, i * 2))];
                let trans = log.start_transaction(change, vec![]);
                trans.commit();
            }
        });
        let join1 = thread::spawn(|| {
            let mut log = FuzzyLog::new(s1, h1, HashMap::new());
            for i in 0..10 {
                let change = vec![(0.into(), MapEntry(i * 2 + 1, i * 2 + 1)), (1.into(), MapEntry(i * 2 + 1, i * 2 + 1))];
                let trans = log.start_transaction(change, vec![]);
                trans.commit();
            }
        });
        join1.join().unwrap();
        join.join().unwrap();
        let mut log = FuzzyLog::new(store, horizon, upcalls);
        log.play_foward(1.into());

        let cannonical_map = {
            let mut map = HashMap::new();
            for i in 0..20 {
                map.insert(i, i);
            }
            map
        };
        //println!("{:#?}", *log.store.lock().unwrap());
        assert_eq!(*map1.borrow(), cannonical_map);
        assert_eq!(*map0.borrow(), cannonical_map);
    }

    #[test]
    fn test_abort_transaction() {
        let _ = env_logger::init();
        let store = new_dyndb_store();
        let horizon = HashMap::new();
        let map = Rc::new(RefCell::new(HashMap::new()));
        let mut upcalls: HashMap<_, Box<Fn(_) -> bool>> = HashMap::new();
        let re = map.clone();
        upcalls.insert(0.into(), Box::new(move |MapEntry(k, v)| {
            re.borrow_mut().insert(k, v);
            true
        }));
        let mut log = FuzzyLog::new(store, horizon, upcalls);
        log.append(0.into(), MapEntry(0, 1), vec![]);
        let mutation = vec![(0.into(), MapEntry(13, 13))];
        log.append(0.into(), MapEntry(1, 17), vec![]);
        drop(log.start_transaction(mutation, vec![]));
        log.play_foward(0.into());
        assert_eq!(*map.borrow(), collect![0 => 1, 1 => 17]);
    }

    #[test]
    fn test_transaction_timeout() {
        use std::mem::forget;
        let _ = env_logger::init();
        let store = new_dyndb_store();
        let horizon = HashMap::new();
        let map = Rc::new(RefCell::new(HashMap::new()));
        let mut upcalls: HashMap<_, Box<Fn(_) -> bool>> = HashMap::new();
        let re = map.clone();
        upcalls.insert(0.into(), Box::new(move |MapEntry(k, v)| {
            trace!("MapEntry({:?}, {:?})", k, v);
            re.borrow_mut().insert(k, v);
            true
        }));
        let mut log = FuzzyLog::new(store, horizon, upcalls);
        log.append(0.into(), MapEntry(0, 1), vec![]);
        log.append(0.into(), MapEntry(1, 17), vec![]);

        let mutation = vec![(0.into(), MapEntry(13, 13))];
        forget(log.start_transaction(mutation, vec![]));

        log.play_foward(0.into());
        assert_eq!(*map.borrow(), collect![0 => 1, 1 => 17]);
    }
}
