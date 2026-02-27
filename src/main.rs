#![allow(unused)]
use std::{collections::HashMap, f64, fs, ops::Index};

mod read;
mod utils;

macro_rules! vec_string {
    ($($str:expr),*) => ({
        vec![$(String::from($str),)*] as Vec<String>
    });
}

static NUM_RECORDS: usize = 14945546;
static START_RECORD: usize = 0;

static TARGETS: [(&str, &usize, [&str; 5]); 1] = [
    (
        "sf",
        &(1124975_usize),
        [
            "spanish",
            "cantonese_mandarin",
            "tagalog_filipino",
            "indic_dravidian",
            "vietnamese",
        ],
    ),
    // (
    //     "tristate",
    //     &(3243704_usize),
    //     [
    //         "spanish",
    //         "cantonese_mandarin",
    //         "indic_dravidian",
    //         "russian",
    //         "italian",
    //     ],
    // ),
    // (
    //     "la",
    //     &(2311222_usize),
    //     [
    //         "spanish",
    //         "cantonese_mandarin",
    //         "tagalog_filipino",
    //         "vietnamese",
    //         "korean",
    //     ],
    // ),
    // (
    //     "chicago",
    //     &(1436110_usize),
    //     [
    //         "spanish",
    //         "polish",
    //         "indic_dravidian",
    //         "cantonese_mandarin",
    //         "tagalog_filipino",
    //     ],
    // ),
    // (
    //     "dallas",
    //     &(1185015_usize),
    //     [
    //         "spanish",
    //         "indic_dravidian",
    //         "vietnamese",
    //         "cantonese_mandarin",
    //         "korean",
    //     ],
    // ),
    // (
    //     "dc",
    //     &(1047707_usize),
    //     [
    //         "spanish",
    //         "indic_dravidian",
    //         "cantonese_mandarin",
    //         "korean",
    //         "vietnamese",
    //     ],
    // ),
    // (
    //     "miami",
    //     &(981666_usize),
    //     [
    //         "spanish",
    //         "portuguese",
    //         "indic_dravidian",
    //         "cantonese_mandarin",
    //         "italian",
    //     ],
    // ),
    // (
    //     "houston",
    //     &(935184_usize),
    //     [
    //         "spanish",
    //         "vietnamese",
    //         "indic_dravidian",
    //         "cantonese_mandarin",
    //         "tagalog_filipino",
    //     ],
    // ),
    // (
    //     "phil",
    //     &(926525_usize),
    //     [
    //         "spanish",
    //         "indic_dravidian",
    //         "cantonese_mandarin",
    //         "italian",
    //         "russian",
    //     ],
    // ),
    // (
    //     "atlanta",
    //     &(900599_usize),
    //     [
    //         "spanish",
    //         "indic_dravidian",
    //         "cantonese_mandarin",
    //         "vietnamese",
    //         "korean",
    //     ],
    // ),
    // (
    //     "boston",
    //     &(852748_usize),
    //     [
    //         "spanish",
    //         "cantonese_mandarin",
    //         "portuguese",
    //         "indic_dravidian",
    //         "italian",
    //     ],
    // ),
];

fn split() {
    let columns = vec![
        0,  // Year                            - 0
        7,  // PUMA                            - 1
        8,  // Metropolitan Area               - 2
        16, // Age                             - 3
        17, // Birthplace                      - 4
        19, // Ancestry                        - 5
        20, // Ancestry Detailed               - 6
        21, // Ancestry 2nd Response           - 7
        22, // Ancestry 2nd Response Detailed  - 8
        23, // Year of Immigration             - 9
        24, // Language                        - 10
        26, // Speaks English                  - 11
    ];

    let key: [(&str, Vec<String>); 11] = [
        ("sf", vec_string!("41860", "41940")),
        ("tristate", vec_string!("35620")),
        ("la", vec_string!("31080")),
        ("chicago", vec_string!("16980")),
        ("dallas", vec_string!("19100")),
        ("houston", vec_string!("26420")),
        ("dc", vec_string!("47900")),
        ("phil", vec_string!("37980")),
        ("miami", vec_string!("33100")),
        ("atlanta", vec_string!("12060")),
        ("boston", vec_string!("14460")),
    ];

    let f = "./data/raw/full.csv";

    let r = read::read(f, columns, &NUM_RECORDS, &START_RECORD);

    if let Ok(data) = r {
        key.iter().for_each(|(name, x)| {
            read::export(
                format!("./data/raw/{}.csv", name),
                read::filter(&(data.0), |y| x.contains(&y[2])),
                &(data.1),
            )
            .unwrap()
        });
    }
}

fn find_large() {
    TARGETS.iter().for_each(|metro| {
        let recs = read::read(
            format!("./data/raw/{}.csv", metro.0),
            (0..12).collect(),
            metro.1,
            &0,
        )
        .unwrap()
        .0;

        let mut langs: HashMap<String, usize> = HashMap::new();

        recs.iter().for_each(|x| {
            langs
                .entry(x[10].clone())
                .and_modify(|x| *x += 1)
                .or_insert(1);
        });

        langs.remove("0");
        langs.remove("1");

        let mut top_lang = langs.into_iter().collect::<Vec<(String, usize)>>();
        top_lang.sort_by(|a, b| (a.1).cmp(&b.1).reverse());

        println!("{} Langs: {:#?}", metro.0, &top_lang[0..10]);
    });
}

fn filter_puma() {
    let filters: HashMap<String, read::Codes> = vec![
        (
            String::from("cantonese_mandarin"),
            read::Codes {
                languages: vec_string!["43"],
                ancestry: vec_string!["706", "707", "708", "709", "716", "782"],
            },
        ),
        (
            String::from("spanish"),
            read::Codes {
                languages: vec_string!["12"],
                ancestry: (200..338).map(|x| x.to_string()).collect(),
            },
        ),
        (
            String::from("tagalog_filipino"),
            read::Codes {
                languages: vec_string!["54"],
                ancestry: vec_string!["720"],
            },
        ),
        (
            String::from("vietnamese"),
            read::Codes {
                languages: vec_string!["50"],
                ancestry: (785..791).map(|x| x.to_string()).collect(),
            },
        ),
        (
            String::from("indic_dravidian"),
            read::Codes {
                languages: vec_string!["31", "40"],
                ancestry: (603..696).map(|x| x.to_string()).collect(),
            },
        ),
        (
            String::from("russian"),
            read::Codes {
                languages: vec_string!["18"],
                ancestry: vec_string!["148"],
            },
        ),
        (
            String::from("italian"),
            read::Codes {
                languages: vec_string!["10"],
                ancestry: (51..73).map(|x| x.to_string()).collect(),
            },
        ),
        (
            String::from("korean"),
            read::Codes {
                languages: vec_string!["49"],
                ancestry: vec_string!["750"],
            },
        ),
        (
            String::from("polish"),
            read::Codes {
                languages: vec_string!["21"],
                ancestry: vec_string!["142"],
            },
        ),
        (
            String::from("portuguese"),
            read::Codes {
                languages: vec_string!["13"],
                ancestry: vec_string!["84", "85", "86", "360"],
            },
        ),
    ]
    .into_iter()
    .collect();

    TARGETS.iter().for_each(|metro| {
        utils::filter_puma(metro.0, metro.1, {
            let mut ret: HashMap<String, &read::Codes> = HashMap::new();
            metro.2.iter().for_each(|x| {
                ret.entry(x.to_string())
                    .insert_entry(filters.get(*x).unwrap());
            });
            ret
        });
    });
}

fn correlation() {
    TARGETS.iter().for_each(|metro| {
        let mut gen_obs: HashMap<String, HashMap<String, utils::Agg>> = HashMap::new();
        let mut loc_obs: HashMap<String, utils::PumaAgg> = HashMap::new();
        let mut loc_mig: HashMap<String, HashMap<usize, usize>> = HashMap::new();

        let paths = fs::read_dir(format!("data/{}/", metro.0)).unwrap();

        paths.for_each(|puma| {
            let m = puma.unwrap();
            if (!m.file_type().unwrap().is_file()) {
                let d = fs::read_dir(m.path()).unwrap();

                d.for_each(|group| {
                    let g = group.unwrap();
                    let name = g.path().file_name().unwrap().to_str().unwrap().to_string();
                    let year = name.split_at(4).0.to_string();
                    if name.ends_with("gen_agg.json") {
                        gen_obs.entry(year).insert_entry(
                            serde_json::from_str(fs::read_to_string(g.path()).unwrap().as_str())
                                .unwrap(),
                        );
                    } else if name.ends_with("_agg.json") {
                        loc_obs.entry(year).insert_entry(
                            serde_json::from_str(fs::read_to_string(g.path()).unwrap().as_str())
                                .unwrap(),
                        );
                    }
                })
            }
        });

        let mut changes: HashMap<String, Vec<(f64, f64)>> = HashMap::new();

        loc_obs.iter().for_each(|(g, obs)| {
            let (year, _) = g.split_at(4);
            let prev_group = (year.parse::<usize>().unwrap() - 1).to_string();
            if (loc_obs.contains_key(&prev_group)) {
                let prev_c = loc_obs.get(&prev_group).unwrap();
                let d_ldi = prev_c.ldi - obs.ldi;
                let d_pop = prev_c.pop as f64 - obs.pop as f64 / prev_c.pop as f64;
                obs.languages.iter().for_each(|(group, agg)| {
                    if (prev_c.languages.contains_key(group)) {
                        let prev = prev_c.languages.get(group).unwrap();

                        changes
                            .entry(format!("{}_ldi", group))
                            .and_modify(|x| x.push((prev.speakers - agg.speakers, d_ldi)))
                            .or_insert(vec![(prev.speakers - agg.speakers, d_ldi)]);

                        changes
                            .entry(format!("{}_pop", group))
                            .and_modify(|x| x.push((prev.speakers - agg.speakers, d_pop)))
                            .or_insert(vec![(prev.speakers - agg.speakers, d_pop)]);

                        changes
                            .entry("overall_pop".to_string())
                            .and_modify(|x| x.push((prev.speakers - agg.speakers, d_pop)))
                            .or_insert(vec![(prev.speakers - agg.speakers, d_pop)]);

                        changes
                            .entry("overall_ldi".to_string())
                            .and_modify(|x| x.push((prev.speakers - agg.speakers, d_ldi)))
                            .or_insert(vec![(prev.speakers - agg.speakers, d_ldi)]);
                    }
                });
            }
        });

        changes.iter().for_each(|(comp, c)| {
            let mut wtr = csv::Writer::from_path(format!("data/{}/{}.csv", metro.0, comp)).unwrap();

            c.iter().for_each(|rec| {
                wtr.write_record(vec![rec.0.to_string(), rec.1.to_string()]);
            });

            wtr.flush().unwrap();

            let mut di: Vec<isize> = Vec::new();
            let mut comp_0: Vec<_> = c.iter().enumerate().collect();
            comp_0.sort_by(|a, b| a.1 .0.total_cmp(&b.1 .0));
            let mut comp_1: Vec<_> = comp_0.iter().enumerate().collect();
            comp_1.sort_by(|a, b| a.1 .1 .1.total_cmp(&b.1 .1 .1));

            comp_1
                .iter()
                .for_each(|a| di.push(a.1 .0 as isize - a.0 as isize));

            println!(
                "{} - {} rho: {}",
                metro.0,
                comp,
                1.0 - (6.0 * di.iter().fold(0.0, |acc, x| acc + (*x as f64).powf(2.0)))
                    / (di.len() as f64 * ((di.len() as f64).powf(2.0) - 1.0))
            );
        });

        let mut gen_changes: HashMap<String, Vec<(f64, f64)>> = HashMap::new();

        gen_obs.iter().for_each(|(g, obs)| {
            let (year, _) = g.split_at(4);
            let prev_group = (year.parse::<usize>().unwrap() - 1).to_string();
            if (gen_obs.contains_key(&prev_group)) {
                let prev_c = gen_obs.get(&prev_group).unwrap();
                obs.iter().for_each(|(group, agg)| {
                    if (prev_c.contains_key(group)) {
                        let prev = prev_c.get(group).unwrap();
                        let change = (agg.speakers - prev.speakers, (agg.pop - prev.pop) as f64);

                        gen_changes
                            .entry(group.clone())
                            .and_modify(|x| x.push(change))
                            .or_insert(vec![change]);
                    }
                });
            }
        });

        gen_changes.iter().for_each(|(comp, c)| {
            let mut wtr = csv::Writer::from_path(format!("data/{}/{}.csv", metro.0, comp)).unwrap();

            c.iter().for_each(|rec| {
                wtr.write_record(vec![rec.0.to_string(), rec.1.to_string()]);
            });

            wtr.flush().unwrap();

            let mut di: Vec<isize> = Vec::new();
            let mut comp_0: Vec<_> = c.iter().enumerate().collect();
            comp_0.sort_by(|a, b| a.1 .0.total_cmp(&b.1 .0));
            let mut comp_1: Vec<_> = comp_0.iter().enumerate().collect();
            comp_1.sort_by(|a, b| a.1 .1 .1.total_cmp(&b.1 .1 .1));

            comp_1
                .iter()
                .for_each(|a| di.push(a.1 .0 as isize - a.0 as isize));

            println!(
                "{} - Gen {} rho: {}",
                metro.0,
                comp,
                1.0 - (6.0 * di.iter().fold(0.0, |acc, x| acc + (*x as f64).powf(2.0)))
                    / (di.len() as f64 * ((di.len() as f64).powf(2.0) - 1.0))
            );
        });
    });
}

fn main() {
    let stages = [correlation];
    stages.iter().for_each(|x| x());
}
