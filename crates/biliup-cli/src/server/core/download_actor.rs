use biliup::client::StatelessClient;

use crate::server::core::StreamStatus;
use crate::server::core::live_streamers::{DynLiveStreamersService, LiveStreamerDto};
use crate::server::core::upload_actor::UploadActorHandle;
use crate::server::core::util::{AnyMap, Cycle, logging_spawn};
use biliup::downloader::extractor::{SiteDefinition, find_extractor};
use biliup::downloader::util::Segmentable;

use indexmap::indexmap;

use std::collections::HashMap;
use std::error::Error;
use std::future::Future;
use std::ops::DerefMut;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::log::info;
use tracing::{debug, error};

async fn finish_download_and_reset<Fut, E>(
    task: &Cycle<StreamStatus>,
    url: &str,
    download_fut: Fut,
) -> Result<(), E>
where
    Fut: Future<Output = Result<(), E>>,
{
    let download_res = download_fut.await;
    task.change(url, StreamStatus::Idle);
    download_res
}

fn reset_status_on_inspect_error(
    task: &Cycle<StreamStatus>,
    url: &str,
    prior_status: StreamStatus,
) {
    if prior_status != StreamStatus::Working {
        task.change(url, StreamStatus::Idle);
    }
}

async fn start_monitor(
    task: Cycle<StreamStatus>,
    extractor: &(dyn SiteDefinition + Send + Sync),
    client: StatelessClient,
    live_streamers_service: DynLiveStreamersService,
) {
    let n = &mut 0;
    loop {
        let Some((url, status)) = task.get(n) else {
            break;
        };
        if status != StreamStatus::Working {
            task.change(&url, StreamStatus::Inspecting);
        }
        match (extractor.get_site(&url, client.clone()).await, status) {
            (Ok(mut site), StreamStatus::Idle | StreamStatus::Inspecting) => {
                println!("Idle\n {url} \n{site}");
                let (filename, split_size, split_time) = if let Ok(LiveStreamerDto {
                    filename,
                    split_size,
                    split_time,
                    ..
                }) =
                    live_streamers_service.get_streamer_by_url(&url).await
                {
                    (filename, split_size, split_time.map(Duration::from_secs))
                } else {
                    ("./video/%Y-%m-%d/%H_%M_%S{title}".to_string(), None, None)
                };
                let still_present = {
                    let mut guard = task.write();
                    if let Some(st) = guard.get_mut(&url) {
                        *st = StreamStatus::Working;
                        true
                    } else {
                        false
                    }
                };
                if !still_present {
                    debug!(url = %url, "streamer removed before download start; skipping");
                    continue;
                }
                let live_streamers_service = live_streamers_service.clone();
                {
                    let client = client.clone();
                    let url = url.clone();
                    let task = task.clone();
                    logging_spawn(async move {
                        let hook = live_streamers_service
                            .get_studio_by_url(&url)
                            .await
                            .unwrap_or_default()
                            .map(|studio| -> Box<dyn Fn(&str) + Send> {
                                let handle = UploadActorHandle::new(client, studio);
                                Box::new(move |file_name| {
                                    if let Ok(metadata) = std::fs::metadata(file_name)
                                        .map_err(|err| error!("{}", err))
                                    {
                                        if metadata.len() > 10 * 1024 * 1024 {
                                            info!("开始上传: {}", file_name);
                                            handle.send_file_path(file_name);
                                        }
                                    }
                                })
                            });
                        if hook.is_none() {
                            debug!(url = %url, "upload template not set.");
                        }

                        let segmentable = Segmentable::new(split_time, split_size);
                        // let segmentable = Segmentable::new( None, Some(16*1024*1024));
                        finish_download_and_reset(
                            &task,
                            &url,
                            site.download(&filename, segmentable, hook),
                        )
                        .await?;
                        Ok::<_, Box<dyn Error + Send + Sync>>(())
                    });
                }
            }
            (Ok(_site), StreamStatus::Pending) => {
                println!("Pending");
            }
            (Ok(_site), StreamStatus::Working) => {
                debug!("Working");
            }
            (Err(e), s) => {
                reset_status_on_inspect_error(&task, &url, s);
                debug!(url, "{e}")
            }
        }
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}

#[derive(Clone)]
struct DownloadActor {
    live_streamers_service: DynLiveStreamersService,
    client: StatelessClient,
}

impl DownloadActor {
    fn new(live_streamers_service: DynLiveStreamersService, client: StatelessClient) -> Self {
        Self {
            live_streamers_service,
            client,
        }
    }

    fn run(
        &mut self,
        list: Vec<LiveStreamerDto>,
        extensions: StreamActorMap,
        // client: StatelessClient,
    ) {
        for streamer in list {
            // let Some(extractor) = find_extractor(&streamer.url) else { continue; };
            let mut guard = extensions.write().unwrap();
            self.add_streamer(guard.deref_mut(), streamer.url)
        }
        println!("{:?}", extensions);
    }

    fn add_streamer(
        &self,
        map: &mut AnyMap<(Cycle<StreamStatus>, JoinHandle<()>)>,
        url: String,
        // client: StatelessClient,
    ) {
        let Some(extractor) = find_extractor(&url) else {
            return;
        };
        let _entry = map
            .entry(extractor.as_any().type_id())
            .and_modify(|(cy, _)| cy.insert(url.clone(), StreamStatus::Idle))
            .or_insert_with(|| {
                let cycle = Cycle::new(indexmap![url => StreamStatus::Idle]);
                let task = cycle.clone();
                let client = self.client.clone();
                let live_streamers_service = self.live_streamers_service.clone();
                let handle = tokio::spawn(async move {
                    start_monitor(task, extractor, client, live_streamers_service).await
                });
                (cycle, handle)
            });
    }
}

type StreamActorMap = Arc<RwLock<AnyMap<(Cycle<StreamStatus>, JoinHandle<()>)>>>;

#[derive(Clone)]
pub struct DownloadActorHandle {
    platform_map: StreamActorMap,
    // client: StatelessClient,
    actor: DownloadActor,
}

impl DownloadActorHandle {
    pub fn new(
        list: Vec<LiveStreamerDto>,
        client: StatelessClient,
        live_streamers_service: DynLiveStreamersService,
    ) -> Self {
        let mut actor = DownloadActor::new(live_streamers_service, client);
        let platform_map = Arc::new(RwLock::new(HashMap::default()));
        let platform = Arc::clone(&platform_map);
        // let client_c = client.clone();
        actor.run(list, platform);
        Self {
            platform_map,
            actor,
        }
    }

    pub fn add_streamer(&self, url: &str) {
        self.actor.add_streamer(
            self.platform_map.write().unwrap().deref_mut(),
            url.to_string(),
            // self.client.clone(),
        );
    }

    pub fn get_streamers(&self) -> HashMap<String, StreamStatus> {
        let read_guard = self.platform_map.read().unwrap();
        let mut map = HashMap::new();
        for (key, val) in read_guard.iter() {
            map.extend(val.0.get_all());
        }
        map
    }

    pub fn update_streamer(&self, url: &str) {
        let Some(extractor) = find_extractor(url) else {
            return;
        };
        let type_id = extractor.as_any().type_id();
        let mut platform_guard = self.platform_map.write().unwrap();
        if let Some((cy, _)) = platform_guard.get(&type_id)
            && cy.write().contains_key(url)
        {
            return;
        }
        self.actor
            .add_streamer(platform_guard.deref_mut(), url.to_string());
    }

    pub fn remove_streamer(&self, url: &str) {
        let Some(extractor) = find_extractor(url) else {
            return;
        };
        let type_id = extractor.as_any().type_id();
        let handle_to_abort = {
            let mut platform_guard = self.platform_map.write().unwrap();
            let should_remove = if let Some((cy, _)) = platform_guard.get(&type_id) {
                let mut cy_guard = cy.write();
                cy_guard.shift_remove(url);
                cy_guard.is_empty()
            } else {
                false
            };
            if should_remove {
                platform_guard.remove(&type_id).map(|(_, handle)| handle)
            } else {
                None
            }
        };
        if let Some(handle) = handle_to_abort {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{finish_download_and_reset, reset_status_on_inspect_error};
    use crate::server::core::StreamStatus;
    use crate::server::core::util::Cycle;
    use indexmap::indexmap;

    #[tokio::test]
    async fn test_finish_download_and_reset_restores_idle_on_ok_and_err() {
        let url = "https://live.bilibili.com/123";
        let cycle = Cycle::new(indexmap! { url.to_string() => StreamStatus::Working });

        let ok_res = finish_download_and_reset(&cycle, url, async { Ok::<(), &str>(()) }).await;
        assert!(ok_res.is_ok());
        assert_eq!(cycle.get_all().get(url), Some(&StreamStatus::Idle));

        cycle.change(url, StreamStatus::Working);
        let err_res = finish_download_and_reset(&cycle, url, async { Err::<(), &str>("boom") }).await;
        assert_eq!(err_res, Err("boom"));
        assert_eq!(cycle.get_all().get(url), Some(&StreamStatus::Idle));
    }

    #[test]
    fn test_reset_status_on_inspect_error_preserves_working_and_resets_others() {
        let url = "https://live.bilibili.com/456";
        let cycle = Cycle::new(indexmap! { url.to_string() => StreamStatus::Working });

        reset_status_on_inspect_error(&cycle, url, StreamStatus::Working);
        assert_eq!(cycle.get_all().get(url), Some(&StreamStatus::Working));

        cycle.change(url, StreamStatus::Inspecting);
        reset_status_on_inspect_error(&cycle, url, StreamStatus::Idle);
        assert_eq!(cycle.get_all().get(url), Some(&StreamStatus::Idle));

        cycle.change(url, StreamStatus::Inspecting);
        reset_status_on_inspect_error(&cycle, url, StreamStatus::Inspecting);
        assert_eq!(cycle.get_all().get(url), Some(&StreamStatus::Idle));
    }
}
