use anyhow::Result;
use std::path::Path;

use crate::market_discovery::Market;

#[derive(Debug, Default)]
pub struct RewardBucket {
    pub label: &'static str,
    pub count: usize,
    pub total_daily_reward: f64,
    pub total_liquidity: f64,
    pub total_volume_24h: f64,
    pub total_volume: f64,
    pub total_max_spread: f64,
    pub max_spread_count: usize,
    pub token_count: usize,
    pub total_competitive: f64,
    pub competitive_count: usize,
}

pub fn analyze_rewards(path: &Path) -> Result<Vec<RewardBucket>> {
    let content = std::fs::read_to_string(path)?;
    let mut markets = Vec::new();
    for line in content.lines() {
        if let Ok(m) = serde_json::from_str::<Market>(line) {
            markets.push(m);
        }
    }

    let mut buckets = vec![
        RewardBucket {
            label: "No Reward",
            ..Default::default()
        },
        RewardBucket {
            label: "Micro (0-5]",
            ..Default::default()
        },
        RewardBucket {
            label: "Small (5-25]",
            ..Default::default()
        },
        RewardBucket {
            label: "Medium (25-100]",
            ..Default::default()
        },
        RewardBucket {
            label: "Large (100-500]",
            ..Default::default()
        },
        RewardBucket {
            label: "Whale (500+)",
            ..Default::default()
        },
    ];

    for m in markets {
        if !m.active {
            continue;
        }
        let daily_rate: f64 = m
            .clob_rewards
            .iter()
            .filter_map(|r| r.rewards_daily_rate)
            .sum();
        let idx = match daily_rate {
            0.0 => 0,
            x if x <= 5.0 => 1,
            x if x <= 25.0 => 2,
            x if x <= 100.0 => 3,
            x if x <= 500.0 => 4,
            _ => 5,
        };

        let b = &mut buckets[idx];
        b.count += 1;
        b.total_daily_reward += daily_rate;
        b.total_liquidity += m.liquidity.unwrap_or(0.0);
        b.total_volume_24h += m.volume_24h.unwrap_or(0.0);
        b.total_volume += m.volume.unwrap_or(0.0);
        b.token_count += m.token_ids.len();
        if let Some(ms) = m.rewards_max_spread {
            b.total_max_spread += ms;
            b.max_spread_count += 1;
        }
        if let Some(c) = m.competitive {
            b.total_competitive += c;
            b.competitive_count += 1;
        }
    }

    Ok(buckets)
}

pub fn print_reward_analysis(buckets: &[RewardBucket]) {
    println!(
        "{:<20} {:>8} {:>14} {:>14} {:>14} {:>14} {:>14} {:>12} {:>14}",
        "Bucket",
        "Markets",
        "DailyReward",
        "AvgLiquidity",
        "AvgVol24h",
        "AvgVolume",
        "AvgMaxSpread",
        "Tokens",
        "AvgCompetitive"
    );
    println!("{}", "-".repeat(136));
    let total_markets: usize = buckets.iter().map(|b| b.count).sum();
    for b in buckets {
        if b.count == 0 {
            continue;
        }
        let avg_liquidity = b.total_liquidity / b.count as f64;
        let avg_vol24h = b.total_volume_24h / b.count as f64;
        let avg_volume = b.total_volume / b.count as f64;
        let avg_max_spread = if b.max_spread_count > 0 {
            b.total_max_spread / b.max_spread_count as f64
        } else {
            0.0
        };
        let avg_competitive = if b.competitive_count > 0 {
            b.total_competitive / b.competitive_count as f64
        } else {
            0.0
        };
        println!(
            "{:<20} {:>8} {:>14.2} {:>14.2} {:>14.2} {:>14.2} {:>14.2} {:>12} {:>14.4}",
            b.label,
            b.count,
            b.total_daily_reward,
            avg_liquidity,
            avg_vol24h,
            avg_volume,
            avg_max_spread,
            b.token_count,
            avg_competitive
        );
    }
    println!("{}", "-".repeat(136));
    println!("Total markets: {}", total_markets);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market_discovery::{ClobReward, Market};
    use tempfile::tempdir;

    fn market(active: bool, daily_rate: f64, spread: Option<f64>, competitive: Option<f64>) -> Market {
        Market {
            id: "id".to_string(),
            condition_id: "cond".to_string(),
            question: "q".to_string(),
            slug: "s".to_string(),
            description: None,
            active,
            closed: false,
            archived: false,
            end_date: None,
            start_date: None,
            created_at: None,
            updated_at: None,
            volume: Some(100.0),
            liquidity: Some(200.0),
            volume_24h: Some(50.0),
            outcomes: None,
            outcome_prices: None,
            token_ids: vec!["t1".to_string()],
            enable_order_book: true,
            order_min_size: None,
            order_price_min_tick_size: None,
            neg_risk: false,
            accepting_orders: true,
            clob_rewards: if daily_rate > 0.0 {
                vec![ClobReward {
                    id: None,
                    condition_id: None,
                    asset_address: None,
                    rewards_amount: None,
                    rewards_daily_rate: Some(daily_rate),
                    start_date: None,
                    end_date: None,
                }]
            } else {
                vec![]
            },
            rewards_min_size: None,
            rewards_max_spread: spread,
            competitive,
        }
    }

    #[test]
    fn test_analyze_rewards_all_buckets() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("markets.jsonl");
        let markets = vec![
            market(true, 0.0, None, None),
            market(true, 3.0, Some(0.01), Some(0.5)),
            market(true, 10.0, Some(0.02), None),
            market(true, 50.0, None, Some(0.7)),
            market(true, 200.0, Some(0.03), None),
            market(true, 600.0, None, Some(0.9)),
        ];
        let mut content = String::new();
        for m in markets {
            content.push_str(&serde_json::to_string(&m).unwrap());
            content.push('\n');
        }
        std::fs::write(&path, content).unwrap();

        let buckets = analyze_rewards(&path).unwrap();
        assert_eq!(buckets[0].count, 1); // No Reward
        assert_eq!(buckets[1].count, 1); // Micro
        assert_eq!(buckets[2].count, 1); // Small
        assert_eq!(buckets[3].count, 1); // Medium
        assert_eq!(buckets[4].count, 1); // Large
        assert_eq!(buckets[5].count, 1); // Whale
    }

    #[test]
    fn test_analyze_rewards_skips_inactive() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("markets.jsonl");
        let m = market(false, 100.0, None, None);
        std::fs::write(&path, serde_json::to_string(&m).unwrap()).unwrap();

        let buckets = analyze_rewards(&path).unwrap();
        assert_eq!(buckets.iter().map(|b| b.count).sum::<usize>(), 0);
    }

    #[test]
    fn test_analyze_rewards_ignores_bad_lines() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("markets.jsonl");
        std::fs::write(&path, "not valid json\n").unwrap();

        let buckets = analyze_rewards(&path).unwrap();
        assert_eq!(buckets.iter().map(|b| b.count).sum::<usize>(), 0);
    }

    #[test]
    fn test_print_reward_analysis() {
        let buckets = vec![
            RewardBucket {
                label: "No Reward",
                count: 0,
                ..Default::default()
            },
            RewardBucket {
                label: "Micro (0-5]",
                count: 2,
                total_daily_reward: 10.0,
                total_liquidity: 400.0,
                total_volume_24h: 100.0,
                total_volume: 200.0,
                total_max_spread: 0.02,
                max_spread_count: 2,
                token_count: 4,
                total_competitive: 1.0,
                competitive_count: 2,
            },
        ];
        // Just ensure it doesn't panic.
        print_reward_analysis(&buckets);
    }
}
