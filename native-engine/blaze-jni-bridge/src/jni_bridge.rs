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

use std::error::Error;

pub use datafusion;
pub use jni::{
    self,
    errors::Result as JniResult,
    objects::{JClass, JMethodID, JObject, JStaticMethodID, JValue},
    signature::{Primitive, ReturnType},
    sys::jvalue,
    JNIEnv, JavaVM,
};
use once_cell::sync::OnceCell;
pub use paste::paste;





#[allow(non_snake_case)]
pub struct JavaClasses<'a> {
    pub cKafkaConsumerManager: KafkaConsumerManager<'a>,
}

#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl<'a> Send for JavaClasses<'a> {}
unsafe impl<'a> Sync for JavaClasses<'a> {}

static JNI_JAVA_CLASSES: OnceCell<JavaClasses> = OnceCell::new();

impl JavaClasses<'static> {
    pub fn init(env: &JNIEnv) {
        if let Err(err) = JNI_JAVA_CLASSES.get_or_try_init(|| -> Result<_, Box<dyn Error>> {
            log::info!("Initializing JavaClasses...");
            let env = unsafe { std::mem::transmute::<_, &'static JNIEnv>(env) };
            let jni_bridge = JniBridge::new(env)?;
            let classloader = env
                .call_static_method_unchecked(
                    jni_bridge.class,
                    jni_bridge.method_getContextClassLoader,
                    jni_bridge.method_getContextClassLoader_ret.clone(),
                    &[],
                )?
                .l()?;

            let java_classes = JavaClasses {
                cKafkaConsumerManager: KafkaConsumerManager::new(env)?,
            };
            log::info!("Initializing JavaClasses finished");
            Ok(java_classes)
        }) {
            log::error!("Initializing JavaClasses error: {err:?}");
            if env.exception_check().unwrap_or(false) {
                let _ = env.exception_describe();
                let _ = env.exception_clear();
            }
            env.fatal_error("Initializing JavaClasses error, cannot continue");
        }
    }

    pub fn inited() -> bool {
        JNI_JAVA_CLASSES.get().is_some()
    }

    pub fn get() -> &'static JavaClasses<'static> {
        unsafe {
            // safety: JNI_JAVA_CLASSES must be initialized frist
            JNI_JAVA_CLASSES.get_unchecked()
        }
    }
}

#[allow(non_snake_case)]
pub struct JniBridge<'a> {
    pub class: JClass<'a>,
    pub method_getKafkaConsumerResource: JStaticMethodID,
    pub method_getKafkaConsumerResource_ret: ReturnType,
}
impl<'a> JniBridge<'a> {
    pub const SIG_TYPE: &'static str = "org/apache/flink/table/planner/plan/nodes/exec/glue/jni/JniBridge";

    pub fn new(env: &JNIEnv<'a>) -> JniResult<JniBridge<'a>> {
        let class = get_global_jclass(env, Self::SIG_TYPE)?;
        Ok(JniBridge {
            class,
            method_getKafkaConsumerResource: env.get_static_method_id(
                class,
                "getKafkaConsumerResource",
                "(Ljava/lang/String;)Ljava/lang/Object;",
            )?,
            method_getKafkaConsumerResource_ret: ReturnType::Object,
        })
    }
}

#[allow(non_snake_case)]
pub struct KafkaConsumerManager<'a> {
    pub class: JClass<'a>,
    pub method_getMessage: JMethodID,
    pub method_getMessage_ret: ReturnType,
    pub method_commit: JMethodID,
    pub method_commit_ret: ReturnType,
}
impl<'a> KafkaConsumerManager<'a> {
    pub const SIG_TYPE: &'static str = "org/apache/flink/table/planner/plan/nodes/exec/glue/jni/KafkaConsumerManager";

    pub fn new(env: &JNIEnv<'a>) -> JniResult<KafkaConsumerManager<'a>> {
        let class = get_global_jclass(env, Self::SIG_TYPE)?;
        Ok(KafkaConsumerManager {
            class,
            method_getMessage: env.get_method_id(
                class,
                "getMessage",
                "()Ljava/nio/ByteBuffer;",
            )?,
            method_getMessage_ret: ReturnType::Object,
            method_commit: env.get_method_id(
                class,
                "commit",
                "()V",
            )?,
            method_commit_ret: ReturnType::Primitive(Primitive::Void),
        })
    }
}
