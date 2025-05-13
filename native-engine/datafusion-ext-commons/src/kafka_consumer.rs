// Copyright 2022 The Blaze Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::slice;
use std::sync::Arc;
use bytes::Bytes;
use blaze_jni_bridge::{jni_call, jni_call_static, jni_get_direct_buffer, jni_new_direct_byte_buffer, jni_new_global_ref, jni_new_object, jni_new_string};
use datafusion::{error::Result, physical_plan::metrics::Time};
use jni::objects::{GlobalRef, JByteBuffer, JObject};
use jni::sys::jlong;
use crate::df_execution_err;
use crate::hadoop_fs::{FsDataInputStream, FsDataOutputStream};
use serde_json::Value;

#[derive(Clone)]
pub struct Ka {
    ka: GlobalRef,
    io_time: Time,
}

impl Ka {
    pub fn new(ka: GlobalRef, io_time_metric: &Time) -> Self {
        Self {
            ka,
            io_time: io_time_metric.clone(),
        }
    }

    pub fn open(&self) -> Result<Value> {
        let _timer = self.io_time.timer();
        let byte_buffer = jni_call!(
            KafkaConsumerManager(self.ka.as_obj()).getMessage() -> JObject
        )?;
        if jni_call!(JavaBuffer(byte_buffer.as_obj()).isDirect() -> bool)? {
            let byte_buffer_global_ref = jni_new_global_ref!(byte_buffer.as_obj())?;
            let data = jni_get_direct_buffer!(byte_buffer_global_ref.as_obj())?;
            let v: Value = serde_json::from_slice(data).unwrap();
            jni_call!(KafkaConsumerManager(self.ka.as_obj()).commit() -> ())?;
            return Ok(v);
        }
        df_execution_err!("ByteBuffer is not direct")
    }
}


// #[derive(Clone)]
// pub struct KaProvier {
//     ka_provider: GlobalRef,
//     io_time: Time,
// }
//
// impl KaProvier {
//     pub fn new(ka_provider: GlobalRef, io_time_metric: &Time) -> Self {
//         Self {
//             ka_provider,
//             io_time: io_time_metric.clone(),
//         }
//     }
//
//     pub fn provide(&self) -> Result<Ka> {
//         let _timer = self.io_time.timer();
//         let ka = jni_call!(
//             ScalaFunction0(self.ka_provider.as_obj()).apply() -> JObject
//         )?;
//         Ok(Ka::new(jni_new_global_ref!(ka.as_obj())?, &self.io_time))
//     }
// }


#[tokio::test]
async fn testwql() -> Result<()> {
    let data: &[u8] = &[91, 123, 34, 110, 97, 109, 101, 34, 58, 34, 49, 34, 44, 34, 97, 103, 101, 34,
        58, 48, 44, 34, 110, 117, 109, 34, 58, 49, 125, 44, 123, 34, 110, 97, 109, 101, 34, 58, 34,
        50, 34, 44, 34, 97, 103, 101, 34, 58, 49, 44, 34, 110, 117, 109, 34, 58, 50, 125, 44, 123,
        34, 110, 97, 109, 101, 34, 58, 34, 51, 34, 44, 34, 97, 103, 101, 34, 58, 48, 44, 34, 110,
        117, 109, 34, 58, 51, 125, 44, 123, 34, 110, 97, 109, 101, 34, 58, 34, 52, 34, 44, 34, 97,
        103, 101, 34, 58, 49, 44, 34, 110, 117, 109, 34, 58, 52, 125, 93
    ];


    // let data = b" {   \"message\": \"Hello world!\"   }";
    let v: Value = serde_json::from_slice(data).unwrap();
    println!("name = {}", v[0]["name"]);
    println!("age  = {}", v[0]["age"]);
    println!("num  = {}", v[0]["num"]);
    println!("name = {}", v[1]["name"]);
    println!("age  = {}", v[1]["age"]);
    println!("num  = {}", v[1]["num"]);
    println!("num  = {}", v.as_array().unwrap().len());



    Ok(())
}