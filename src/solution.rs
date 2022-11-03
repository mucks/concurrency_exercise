use std::collections::HashMap;

use crate::statement::*;
use async_trait::async_trait;
use tokio::{
    sync::mpsc::Sender,
    task::{self, JoinHandle},
};

#[async_trait]
pub trait Solution {
    async fn solve(repositories: Vec<ServerName>) -> Option<Binary>;
}

pub struct Solution0;

#[async_trait]
impl Solution for Solution0 {
    async fn solve(repositories: Vec<ServerName>) -> Option<Binary> {
        return solve_with_bonus(repositories).await;
    }
}

fn start_task(
    tx: Sender<(String, Result<Binary, ServerError>)>,
    repo: ServerName,
) -> JoinHandle<()> {
    // create an async task with tokio to run the download function
    task::spawn(async move {
        // download the binary and store the result
        let result = download(repo.to_owned()).await;
        // send the repo_name alongside the result in a tuple
        if let Err(e) = tx.send((repo.0.to_owned(), result)).await {
            // panic here because this should never happen in this application
            panic!("{}", e);
        }
    })
}

// Solve the problem without the Bonus Task
async fn solve(repositories: Vec<ServerName>) -> Option<Binary> {
    // Get amount of repositories here so I can use that information later
    let repository_amount = repositories.len();

    // Create a new HashMap to store tasks with repository_name as index
    let mut tasks = HashMap::new();

    // create a new channel with capacity 100 (I just chose 100 as an arbitary value, it might not be the best channel capacity in this use case)
    let (tx, mut rx) = tokio::sync::mpsc::channel(100);

    // iterate through all repositories
    for repo in repositories {
        // create an async task and save the handle in the hashmap tasks
        tasks.insert(repo.0.to_owned(), start_task(tx.clone(), repo.clone()));
    }

    // create a fails array to check how many repositories have failed
    let mut fails = vec![];

    // run and endless loop that we will break through a return when the required conditions are met
    loop {
        // receive repo_name and result from the running tasks closures
        if let Some((repo_name, result)) = rx.recv().await {
            match result {
                Ok(binary) => {
                    // Ensure all other tasks are aborted before returning binary
                    tasks.iter().for_each(|(_repo_name, task)| task.abort());
                    // break the loop by returning the binary
                    return Some(binary);
                }
                Err(e) => {
                    println!("{}", e);

                    // add the error to the fails array
                    fails.push(e);
                    // log the amount of repositories that have failed
                    println!("{} repositories have failed", fails.len());

                    // if task is found abort it and remove it from array
                    if let Some(task) = tasks.get(&repo_name) {
                        task.abort();
                        tasks.remove(&repo_name);
                    }

                    // If all repositories fail return None
                    if fails.len() == repository_amount {
                        return None;
                    }
                }
            }
        }
    }
}

//Solve the problem with the bonus task (retry download if fail)
//Note: See solve() function for comments of the first part
async fn solve_with_bonus(repositories: Vec<ServerName>) -> Option<Binary> {
    let mut tasks = HashMap::new();

    let (tx, mut rx) = tokio::sync::mpsc::channel(1);

    for repo in &repositories {
        tasks.insert(repo.0.to_owned(), start_task(tx.clone(), repo.clone()));
    }

    loop {
        if let Some((repo_name, result)) = rx.recv().await {
            match result {
                Ok(binary) => {
                    tasks.iter().for_each(|(_repo_name, task)| task.abort());
                    return Some(binary);
                }
                Err(e) => {
                    println!("{}", e);
                    // get failed task
                    if let Some(task) = tasks.get(&repo_name) {
                        // ensure task is completed
                        task.abort();
                        // remove task from task list
                        tasks.remove(&repo_name);

                        // find correct repository by repo_name ( I could just create a new repo but, in case the repository struct had more values, then this current solution would also work )
                        if let Some(repo) = repositories.iter().find(|r| r.0 == repo_name) {
                            println!("Restarting Download from Repository {}", &repo.0);
                            // restart task of failed repository
                            tasks.insert(repo.0.to_owned(), start_task(tx.clone(), repo.clone()));
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::solve;
    use crate::{solution::solve_with_bonus, statement::ServerName};

    fn create_some_repositories() -> Vec<ServerName> {
        let mut repositories = vec![];
        for i in 0..5 {
            repositories.push(ServerName(format!("Server-{}", i)));
        }
        repositories
    }

    fn create_many_repositories() -> Vec<ServerName> {
        let mut repositories = vec![];
        for i in 0..10000 {
            repositories.push(ServerName(format!("Server-{}", i)));
        }
        repositories
    }

    #[tokio::test]
    async fn test_solve_in_parallel() {
        let mut tasks = vec![];
        let repos = create_some_repositories();
        for _i in 0..20 {
            let repos_clone = repos.clone();
            tasks.push(tokio::task::spawn(async move { solve(repos_clone).await }));
        }

        for task in tasks {
            let res = task.await;
            assert!(res.is_ok());
        }
    }

    #[tokio::test]
    async fn test_solve_bonus_in_parallel() {
        let mut tasks = vec![];
        let repos = create_some_repositories();
        for _i in 0..20 {
            let repos_clone = repos.clone();
            tasks.push(tokio::task::spawn(async move {
                solve_with_bonus(repos_clone).await
            }));
        }

        for task in tasks {
            let res = task.await;
            assert!(res.is_ok());

            if let Ok(binary) = res {
                assert!(binary.is_some());
            }
        }
    }

    #[tokio::test]
    async fn test_solve_one_repository_many_times() {
        let repositories = vec![ServerName("Server".into())];
        for i in 0..20 {
            println!("Iteration {}: test_solve_one_repository_many_times", i);
            let repositories_clone = repositories.clone();
            let binary = solve(repositories_clone).await;
            assert!(binary.is_some() || binary.is_none());
        }
    }

    #[tokio::test]
    async fn test_solve_bonus_one_repository_many_times() {
        let repositories = vec![ServerName("Server".into())];
        for i in 0..20 {
            println!("Iteration {}: test_solve_one_repository_many_times", i);
            let repositories_clone = repositories.clone();
            let binary = solve_with_bonus(repositories_clone).await;
            assert!(binary.is_some());
        }
    }

    #[tokio::test]
    async fn test_solve_many_repositories() {
        let repositories = create_many_repositories();
        let binary = solve(repositories).await;

        assert!(binary.is_some() || binary.is_none());
    }

    #[tokio::test]
    async fn test_solve_bonus_many_repositories() {
        let repositories = create_many_repositories();
        let binary = solve_with_bonus(repositories).await;

        assert!(binary.is_some());
    }
}
