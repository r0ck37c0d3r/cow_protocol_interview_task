use std::{
    collections::HashMap,
    result::Result,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use colour::{blue_ln, green_ln, red_ln};
use futures::{stream::BoxStream, StreamExt};

type City = String;
type Temperature = u64;

#[async_trait]
pub trait Api: Send + Sync + 'static {
    async fn fetch(&self) -> Result<HashMap<City, Temperature>, String>;
    async fn subscribe(&self) -> BoxStream<Result<(City, Temperature), String>>;
}

pub struct StreamCache {
    results: Arc<Mutex<HashMap<String, u64>>>,
}

impl StreamCache {
    pub fn new(api: impl Api) -> Self {
        println!("StreamCache::new");
        let instance = Self {
            results: Arc::new(Mutex::new(HashMap::new())),
        };
        instance.update_in_background(api);
        instance
    }

    pub fn get(&self, key: &str) -> Option<u64> {
        let results = self.results.lock().expect("poisoned");
        results.get(key).copied()
    }

    /// will update results with temperatures from the API
    pub fn update_in_background(&self, api: impl Api) {
        println!("update_in_background");

        // async coroutines used in tokio::spawn take ownership, so create
        // clones of Arc beforehand
        let results_for_fetch = self.results.clone();
        let results_for_subscribe = self.results.clone();

        // in order to share `api` between tokio tasks, wrap it in an arc
        let api = Arc::new(api);
        let api_for_fetch = api.clone();

        // initial fetch to populate database
        tokio::task::spawn(async move {
            if let Ok(new_data) = api_for_fetch.fetch().await {
                green_ln!("Initial fetch");
                let mut existing_map = results_for_fetch.lock().expect("poisoned");

                // .fetch may be handling outdated temps because it fetches all
                // temps, so no overwriting: only add new cities,
                for (city, temp) in new_data {
                    existing_map.entry(city).or_insert(temp);
                }
            }
        });

        // background task which updates a city whenever its temperature changes
        tokio::task::spawn(async move {
            loop {
                let stream = api.subscribe().await;
                green_ln!("Got response from .subscribe");

                // clone arc before moving into the closure
                let arc = results_for_subscribe.clone();

                stream
                    .for_each_concurrent(None, |res| async {
                        match res {
                            Ok((city, temp)) => {
                                blue_ln!("Updating {} to {}", city, temp);
                                // critical section
                                let mut current_values = arc.lock().expect("poisoned");
                                current_values
                                    .entry(city)
                                    .and_modify(|curr_temp| {
                                        *curr_temp = temp;
                                    })
                                    .or_insert(temp);
                            }
                            Err(err) => {
                                red_ln!("Error from stream: {}", err)
                            }
                        }
                    })
                    .await;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::{future, stream::select, FutureExt, StreamExt};
    use maplit::hashmap;
    use tokio::{sync::Notify, time};

    use super::*;

    #[derive(Default)]
    struct TestApi {
        signal: Arc<Notify>,
    }

    #[async_trait]
    impl Api for TestApi {
        async fn fetch(&self) -> Result<HashMap<City, Temperature>, String> {
            // fetch is slow an may get delayed until after we receive the first
            // updates
            self.signal.notified().await;
            Ok(hashmap! {
                "Berlin".to_string() => 29,
                "Paris".to_string() => 31,
            })
        }
        async fn subscribe(&self) -> BoxStream<Result<(City, Temperature), String>> {
            let results = vec![
                Ok(("London".to_string(), 27)),
                Ok(("Paris".to_string(), 32)),
            ];
            select(
                futures::stream::iter(results),
                async {
                    self.signal.notify_one();
                    future::pending().await
                }
                .into_stream(),
            )
            .boxed()
        }
    }
    #[tokio::test]
    /// Test assumes correct value for Paris is 32 as delivered by
    /// .subscribe. But .subscribe needs to be invoked first in order for .fetch
    /// to send a value, which means .fetch would give a later, "more recent"
    /// value. As per README instructions:
    ///
    /// > The goal of the cache is to always return the most recent value the
    /// > API has delivered.
    ///
    /// However, because .fetch collects ALL temperatures, an update to one of
    /// the temperatures could've happened before .fetch returned its (outdated)
    /// data, resulting in TOCTOU race condition.
    ///
    /// We cannot know which value was more recent. Simplest assumption is to
    /// make .subscribe calls overwrite .fetch information, but not vice versa.
    /// This is because .subscribe always returns the most recent value, while
    /// .fetch may not
    ///
    /// It's a bit of a dirty solution because if .fetch returns quickly before
    /// the next .subscribe cycle, there is a brief period where a user may
    /// get outdated info, because we discarded .fetch's data. But due to
    /// .subscribe running constantly, this'd be a rare occurrence.
    ///
    /// A better solution exists which requires new data. If we kept track of
    /// when the value was obtained, we'd always know what the most recent
    /// temperature was...
    /// ...assuming we trust the clocks are synchronized :)
    async fn works() {
        let cache = StreamCache::new(TestApi::default());

        // Allow cache to update
        time::sleep(Duration::from_millis(100)).await;

        assert_eq!(cache.get("Berlin"), Some(29));
        assert_eq!(cache.get("London"), Some(27));
        assert_eq!(cache.get("Paris"), Some(32));
    }
}
