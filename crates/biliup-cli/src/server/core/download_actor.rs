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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::log::info;
use tracing::{debug, error};

static DOWNLOAD_GENERATION: AtomicU64 = AtomicU64::new(1);
type DownloadGenerations = Arc<RwLock<HashMap<String, u64>>>;

/// RAII guard that binds a download task to a unique generation and resets
/// `StreamStatus::Idle` when dropped unless superseded or removed.
struct DownloadStatusGuard {
    task: Cycle<StreamStatus>,
    generations: DownloadGenerations,
    url: String,
    generation: u64,
}

impl DownloadStatusGuard {
    fn try_claim(
        task: &Cycle<StreamStatus>,
        generations: &DownloadGenerations,
        url: &str,
    ) -> Option<Self> {
        let mut status_guard = task.write();
        let st = status_guard.get_mut(url)?;
        if *st == StreamStatus::Working {
            return None;
        }
        *st = StreamStatus::Working;
        let generation = DOWNLOAD_GENERATION.fetch_add(1, Ordering::Relaxed);
        generations
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .insert(url.to_string(), generation);
        Some(Self {
            task: task.clone(),
            generations: Arc::clone(generations),
            url: url.to_string(),
            generation,
        })
    }
}

impl Drop for DownloadStatusGuard {
    fn drop(&mut self) {
        let mut status_guard = self.task.write();
        let mut gen_guard = self
            .generations
            .write()
            .unwrap_or_else(|p| p.into_inner());
        if gen_guard.get(&self.url).copied() == Some(self.generation) {
            gen_guard.remove(&self.url);
            if let Some(st) = status_guard.get_mut(&self.url) {
                *st = StreamStatus::Idle;
            }
        }
    }
}

async fn finish_download_and_reset<Fut, E>(
    guard: DownloadStatusGuard,
    download_fut: Fut,
) -> Result<(), E>
where
    Fut: Future<Output = Result<(), E>>,
{
    let _guard = guard;
    download_fut.await
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
    generations: DownloadGenerations,
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
                let Some(status_guard) = DownloadStatusGuard::try_claim(&task, &generations, &url)
                else {
                    debug!(
                        url = %url,
                        "streamer removed or already working before download start; skipping"
                    );
                    continue;
                };
                let live_streamers_service = live_streamers_service.clone();
                {
                    let client = client.clone();
                    let url = url.clone();
                    logging_spawn(async move {
                        let status_guard = status_guard;
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
                            status_guard,
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
    generations: DownloadGenerations,
}

impl DownloadActor {
    fn new(
        live_streamers_service: DynLiveStreamersService,
        client: StatelessClient,
        generations: DownloadGenerations,
    ) -> Self {
        Self {
            live_streamers_service,
            client,
            generations,
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
            .and_modify(|(cy, _)| {
                cy.write().entry(url.clone()).or_insert(StreamStatus::Idle);
            })
            .or_insert_with(|| {
                let cycle = Cycle::new(indexmap![url => StreamStatus::Idle]);
                let task = cycle.clone();
                let client = self.client.clone();
                let live_streamers_service = self.live_streamers_service.clone();
                let generations = Arc::clone(&self.generations);
                let handle = tokio::spawn(async move {
                    start_monitor(task, extractor, client, live_streamers_service, generations)
                        .await
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
    generations: DownloadGenerations,
}

impl DownloadActorHandle {
    pub fn new(
        list: Vec<LiveStreamerDto>,
        client: StatelessClient,
        live_streamers_service: DynLiveStreamersService,
    ) -> Self {
        let generations = Arc::new(RwLock::new(HashMap::new()));
        let mut actor = DownloadActor::new(live_streamers_service, client, Arc::clone(&generations));
        let platform_map = Arc::new(RwLock::new(HashMap::default()));
        let platform = Arc::clone(&platform_map);
        // let client_c = client.clone();
        actor.run(list, platform);
        Self {
            platform_map,
            actor,
            generations,
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
        for (_key, val) in read_guard.iter() {
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
                self.generations
                    .write()
                    .unwrap_or_else(|p| p.into_inner())
                    .remove(url);
                cy_guard.is_empty()
            } else {
                self.generations
                    .write()
                    .unwrap_or_else(|p| p.into_inner())
                    .remove(url);
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
    use super::{
        DownloadActor, DownloadActorHandle, DownloadGenerations, DownloadStatusGuard,
        finish_download_and_reset, reset_status_on_inspect_error,
    };
    use crate::server::core::StreamStatus;
    use crate::server::core::live_streamers::{
        AddLiveStreamerDto, LiveStreamerDto, LiveStreamersService,
    };
    use crate::server::core::util::{AnyMap, Cycle};
    use async_trait::async_trait;
    use biliup::client::StatelessClient;
    use biliup::downloader::extractor::find_extractor;
    use biliup::uploader::bilibili::Studio;
    use indexmap::indexmap;
    use reqwest::header::HeaderMap;
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};

    struct DummyLiveStreamersService;

    #[async_trait]
    impl LiveStreamersService for DummyLiveStreamersService {
        async fn add_streamer(
            &self,
            _request: AddLiveStreamerDto,
        ) -> anyhow::Result<LiveStreamerDto> {
            Err(anyhow::anyhow!("not used"))
        }

        async fn get_streamer_by_url(&self, _url: &str) -> anyhow::Result<LiveStreamerDto> {
            Err(anyhow::anyhow!("not used"))
        }

        async fn get_streamer_by_id(&self, _id: i64) -> anyhow::Result<LiveStreamerDto> {
            Err(anyhow::anyhow!("not used"))
        }

        async fn get_streamers(&self) -> anyhow::Result<Vec<LiveStreamerDto>> {
            Ok(vec![])
        }

        async fn get_studio_by_url(&self, _url: &str) -> anyhow::Result<Option<Studio>> {
            Ok(None)
        }
    }

    fn new_test_generations() -> DownloadGenerations {
        Arc::new(RwLock::new(HashMap::new()))
    }

    fn new_test_client() -> StatelessClient {
        StatelessClient::new(HeaderMap::new(), None)
    }

    #[tokio::test]
    async fn test_finish_download_and_reset_restores_idle_on_ok_and_err() {
        let url = "https://live.bilibili.com/123";
        let cycle = Cycle::new(indexmap! { url.to_string() => StreamStatus::Idle });
        let generations = new_test_generations();

        let guard = DownloadStatusGuard::try_claim(&cycle, &generations, url).unwrap();
        assert_eq!(cycle.get_all().get(url), Some(&StreamStatus::Working));
        assert!(DownloadStatusGuard::try_claim(&cycle, &generations, url).is_none());

        let ok_res = finish_download_and_reset(guard, async { Ok::<(), &str>(()) }).await;
        assert!(ok_res.is_ok());
        assert_eq!(cycle.get_all().get(url), Some(&StreamStatus::Idle));
        assert!(!generations.read().unwrap().contains_key(url));

        let guard = DownloadStatusGuard::try_claim(&cycle, &generations, url).unwrap();
        assert_eq!(cycle.get_all().get(url), Some(&StreamStatus::Working));
        let err_res = finish_download_and_reset(guard, async { Err::<(), &str>("boom") }).await;
        assert_eq!(err_res, Err("boom"));
        assert_eq!(cycle.get_all().get(url), Some(&StreamStatus::Idle));
        assert!(!generations.read().unwrap().contains_key(url));
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

    #[tokio::test]
    async fn test_add_streamer_does_not_overwrite_working_status() {
        let url_working = "https://live.bilibili.com/123";
        let url_new = "https://live.bilibili.com/456";
        let type_id = find_extractor(url_working).unwrap().as_any().type_id();

        let cycle = Cycle::new(indexmap! { url_working.to_string() => StreamStatus::Working });
        let dummy_handle = tokio::spawn(std::future::pending::<()>());
        let mut map = AnyMap::default();
        map.insert(type_id, (cycle.clone(), dummy_handle));

        let actor = DownloadActor::new(
            Arc::new(DummyLiveStreamersService),
            new_test_client(),
            new_test_generations(),
        );

        actor.add_streamer(&mut map, url_working.to_string());
        assert_eq!(cycle.get_all().get(url_working), Some(&StreamStatus::Working));

        actor.add_streamer(&mut map, url_new.to_string());
        assert_eq!(cycle.get_all().get(url_working), Some(&StreamStatus::Working));
        assert_eq!(cycle.get_all().get(url_new), Some(&StreamStatus::Idle));

        if let Some((_, handle)) = map.remove(&type_id) {
            handle.abort();
        }
    }

    #[tokio::test]
    async fn test_remove_readd_and_second_download_ignores_stale_first_finish() {
        let url_a = "https://live.bilibili.com/1001";
        let url_b = "https://live.bilibili.com/1002";
        let type_id = find_extractor(url_a).unwrap().as_any().type_id();

        let cycle = Cycle::new(indexmap! {
            url_a.to_string() => StreamStatus::Idle,
            url_b.to_string() => StreamStatus::Idle,
        });
        let dummy_handle = tokio::spawn(std::future::pending::<()>());
        let mut map = AnyMap::default();
        map.insert(type_id, (cycle.clone(), dummy_handle));

        let generations = new_test_generations();
        let actor = DownloadActor::new(
            Arc::new(DummyLiveStreamersService),
            new_test_client(),
            Arc::clone(&generations),
        );
        let handle = DownloadActorHandle {
            platform_map: Arc::new(RwLock::new(map)),
            actor,
            generations: Arc::clone(&generations),
        };

        let guard1 = DownloadStatusGuard::try_claim(&cycle, &handle.generations, url_a).unwrap();
        let (tx1, rx1) = tokio::sync::oneshot::channel::<()>();
        let task1 = tokio::spawn(finish_download_and_reset(guard1, async move {
            let _ = rx1.await;
            Ok::<(), &str>(())
        }));

        handle.remove_streamer(url_a);
        assert!(!handle.get_streamers().contains_key(url_a));
        assert!(!handle.generations.read().unwrap().contains_key(url_a));

        handle.add_streamer(url_a);
        assert_eq!(handle.get_streamers().get(url_a), Some(&StreamStatus::Idle));

        let guard2 = DownloadStatusGuard::try_claim(&cycle, &handle.generations, url_a).unwrap();
        let gen2 = guard2.generation;
        let (tx2, rx2) = tokio::sync::oneshot::channel::<()>();
        let task2 = tokio::spawn(finish_download_and_reset(guard2, async move {
            let _ = rx2.await;
            Ok::<(), &str>(())
        }));

        let _ = tx1.send(());
        task1.await.unwrap().unwrap();
        assert_eq!(
            handle.get_streamers().get(url_a),
            Some(&StreamStatus::Working)
        );
        assert_eq!(
            handle.generations.read().unwrap().get(url_a).copied(),
            Some(gen2)
        );

        let _ = tx2.send(());
        task2.await.unwrap().unwrap();
        assert_eq!(handle.get_streamers().get(url_a), Some(&StreamStatus::Idle));
        assert!(!handle.generations.read().unwrap().contains_key(url_a));

        if let Some((_, monitor_handle)) = handle.platform_map.write().unwrap().remove(&type_id) {
            monitor_handle.abort();
        }
    }

    #[tokio::test]
    async fn test_finish_download_and_reset_recovers_idle_on_panic() {
        let url = "https://live.bilibili.com/789";
        let cycle = Cycle::new(indexmap! { url.to_string() => StreamStatus::Idle });
        let generations = new_test_generations();

        let guard = DownloadStatusGuard::try_claim(&cycle, &generations, url).unwrap();
        assert_eq!(cycle.get_all().get(url), Some(&StreamStatus::Working));

        let task = tokio::spawn(finish_download_and_reset(guard, async {
            panic!("boom");
            #[allow(unreachable_code)]
            Ok::<(), &str>(())
        }));

        let join_err = task.await.unwrap_err();
        assert!(join_err.is_panic());
        assert_eq!(cycle.get_all().get(url), Some(&StreamStatus::Idle));
        assert!(!generations.read().unwrap().contains_key(url));
    }
}
