pub mod script;

use crate::backend::pool::BackendPool;
use crate::config::HookConfig;
use crate::config::HookPluginConfig;
use std::collections::HashMap;
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub enum HookEvent {
    BackendFailed(String, Vec<String>),
    BackendRecovered(String, Vec<String>),
    GroupFailed(String, Vec<String>),
    GroupRecovered(String, Vec<String>),
}

impl HookEvent {
    pub fn description(&self) -> String {
        match self {
            HookEvent::BackendFailed(name, _) => format!("Backend {} fails", name),
            HookEvent::BackendRecovered(name, _) => format!("Backend {} recovers", name),
            HookEvent::GroupFailed(name, _) => format!("Group {} fails", name),
            HookEvent::GroupRecovered(name, _) => format!("Group {} recovers", name),
        }
    }
}

pub trait HookPlugin: Send + Sync {
    fn on_event<'a>(
        &'a self,
        event: &'a HookEvent,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>>;
}

pub struct HookManager {
    plugins: HashMap<String, Box<dyn HookPlugin>>,
    rx: mpsc::UnboundedReceiver<HookEvent>,
    pool: BackendPool,
    group_healthy_state: HashMap<String, bool>,
}

impl HookManager {
    pub fn new(
        configs: &[HookConfig],
        rx: mpsc::UnboundedReceiver<HookEvent>,
        pool: BackendPool,
    ) -> Self {
        let mut plugins: HashMap<String, Box<dyn HookPlugin>> = HashMap::new();
        for config in configs {
            match &config.plugin {
                HookPluginConfig::Script { script_path } => {
                    plugins.insert(
                        config.name.clone(),
                        Box::new(script::ScriptPlugin::new(script_path.clone())),
                    );
                }
            }
        }
        Self {
            plugins,
            rx,
            pool,
            group_healthy_state: HashMap::new(),
        }
    }

    pub async fn run(mut self) {
        // Initialize group states based on current health
        let initial_groups = self.pool.get_groups_snapshot().await;
        for (group_name, (is_healthy, _)) in initial_groups {
            self.group_healthy_state.insert(group_name, is_healthy);
        }

        while let Some(event) = self.rx.recv().await {
            tracing::debug!("Received hook event: {:?}", event);
            self.dispatch(&event).await;

            // Re-evaluate group health after any backend change
            self.check_groups().await;
        }
    }

    async fn dispatch(&self, event: &HookEvent) {
        let hooks_to_run = match event {
            HookEvent::BackendFailed(_, hooks) => hooks,
            HookEvent::BackendRecovered(_, hooks) => hooks,
            HookEvent::GroupFailed(_, hooks) => hooks,
            HookEvent::GroupRecovered(_, hooks) => hooks,
        };
        let mut unique_hooks = hooks_to_run.clone();
        unique_hooks.sort();
        unique_hooks.dedup();
        for hook_name in unique_hooks {
            if let Some(plugin) = self.plugins.get(&hook_name) {
                plugin.on_event(event).await;
            }
        }
    }

    async fn check_groups(&mut self) {
        let current_groups = self.pool.get_groups_snapshot().await;
        for (group_name, (is_healthy, hooks)) in current_groups {
            let prev_healthy = self
                .group_healthy_state
                .get(&group_name)
                .copied()
                .unwrap_or(true);
            if prev_healthy != is_healthy {
                self.group_healthy_state
                    .insert(group_name.clone(), is_healthy);
                let event = if is_healthy {
                    HookEvent::GroupRecovered(group_name.clone(), hooks)
                } else {
                    HookEvent::GroupFailed(group_name.clone(), hooks)
                };
                self.dispatch(&event).await;
            }
        }
    }
}
