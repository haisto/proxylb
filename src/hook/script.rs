use super::{HookEvent, HookPlugin};
use tokio::process::Command;
use tracing::{error, info};

pub struct ScriptPlugin {
    script_path: String,
}

impl ScriptPlugin {
    pub fn new(script_path: String) -> Self {
        Self { script_path }
    }
}

impl HookPlugin for ScriptPlugin {
    fn on_event<'a>(
        &'a self,
        event: &'a HookEvent,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let arg = event.description();
            info!(
                "Executing script hook: {} with arg: '{}'",
                self.script_path, arg
            );

            let path = self.script_path.clone();
            tokio::spawn(async move {
                match Command::new(&path).arg(&arg).output().await {
                    Ok(out) => {
                        if out.status.success() {
                            let stdout_str = String::from_utf8_lossy(&out.stdout);
                            let stderr_str = String::from_utf8_lossy(&out.stderr);

                            let mut msg = format!("Script hook '{}' succeeded.", path);
                            if !stdout_str.trim().is_empty() {
                                msg.push_str(&format!(" Output:\n{}", stdout_str.trim()));
                            }
                            if !stderr_str.trim().is_empty() {
                                msg.push_str(&format!(" Stderr:\n{}", stderr_str.trim()));
                            }
                            info!("{}", msg);
                        } else {
                            let stderr_str = String::from_utf8_lossy(&out.stderr);
                            let stdout_str = String::from_utf8_lossy(&out.stdout);
                            error!(
                                "Script hook '{}' failed with status: {}. Stderr:\n{} Stdout:\n{}",
                                path,
                                out.status,
                                stderr_str.trim(),
                                stdout_str.trim()
                            );
                        }
                    }
                    Err(e) => {
                        error!("Failed to execute script hook '{}': {}", path, e);
                    }
                }
            });
        })
    }
}
