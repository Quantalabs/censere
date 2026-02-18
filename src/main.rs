use std::{collections::HashMap, fs};

mod read;
mod utils;

macro_rules! vec_string {
    ($($str:expr),*) => ({
        vec![$(String::from($str),)*] as Vec<String>
    });
}

static NUM_RECORDS: usize = 14945546;
static START_RECORD: usize = 0;

static TARGETS: [(&str, &usize); 11] = [
    ("sf", &(1124976_usize)),
    ("tristate", &(3243705_usize)),
    ("la", &(2311223_usize)),
    ("chicago", &(1436111_usize)),
    ("dallas", &(1185016_usize)),
    ("dc", &(1047708_usize)),
    ("miami", &(981667_usize)),
    ("houston", &(935185_usize)),
    ("phil", &(926526_usize)),
    ("atlanta", &(900600_usize)),
    ("boston", &(852749_usize)),
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

fn filter_languages() {
    let filters: Vec<read::FilterLang> = vec![
        read::FilterLang(
            String::from("cantonese_mandarin"),
            read::Codes {
                languages: vec_string!["43"],
                ancestry: vec_string!["706", "707", "708", "709", "716", "782"],
            },
        ),
        read::FilterLang(
            String::from("spanish"),
            read::Codes {
                languages: vec_string!["12"],
                ancestry: (200..338).map(|x| x.to_string()).collect(),
            },
        ),
        read::FilterLang(
            String::from("tagalog_filipino"),
            read::Codes {
                languages: vec_string!["54"],
                ancestry: vec_string!["720"],
            },
        ),
        read::FilterLang(
            String::from("vietnamese"),
            read::Codes {
                languages: vec_string!["50"],
                ancestry: (785..791).map(|x| x.to_string()).collect(),
            },
        ),
        read::FilterLang(
            String::from("indic_dravidian"),
            read::Codes {
                languages: vec_string!["31", "40"],
                ancestry: (603..696).map(|x| x.to_string()).collect(),
            },
        ),
    ];

    for metro in TARGETS {
        read::filter_metro(String::from(metro.0), &filters, metro.1, &0);
    }
}

fn filter_puma() {
    let languages = [
        "cantonese_mandarin",
        "spanish",
        "tagalog_filipino",
        "vietnamese",
        "indic_dravidian",
    ];

    TARGETS.iter().for_each(|metro| {
        languages.iter().for_each(|lang| {
            utils::filter_puma(metro.0, lang);
        });

        let mut proportions: HashMap<String, (f64, HashMap<String, f64>)> = HashMap::new();
        let mut pops: HashMap<String, usize> = HashMap::new();
        let raw = read::read_single(format!("./data/raw/{}.csv", metro.0), (0..12).collect())
            .unwrap()
            .0;

        raw.iter().for_each(|record| {
            let lang = if record[10] == "01" {
                String::from("00")
            } else {
                record[10].clone()
            };

            proportions
                .entry(lang)
                .and_modify(|p| {
                    p.0 += 1.0;
                    p.1.insert(record[1].clone(), 1.0);
                })
                .or_insert((1.0, HashMap::from([(record[1].clone(), 1.0)])));

            pops.entry(record[1].clone())
                .and_modify(|p| *p += 1)
                .or_insert(1);
        });

        proportions.clone().iter().for_each(|(lang, (p, m))| {
            let mut new = m.clone();

            m.iter().for_each(|(puma, pop)| {
                new.entry(puma.clone())
                    .and_modify(|e| *e = pop / (*pops.get(puma).unwrap() as f64));

                proportions
                    .entry(lang.clone())
                    .and_modify(|e| *e = (p / (*metro.1 as f64), new.clone()));
            });
        });

        let mut ldi: HashMap<String, f64> = HashMap::new();

        proportions.iter().for_each(|(_, (p, m))| {
            ldi.entry(String::from(metro.0))
                .and_modify(|e| *e -= p.powf(2.0))
                .or_insert(1.0 - p.powf(2.0));

            m.iter().for_each(|(met, pro)| {
                ldi.entry(String::from(met))
                    .and_modify(|e| *e -= pro.powf(2.0))
                    .or_insert(1.0 - pro.powf(2.0));
            });
        });

        fs::write(
            format!("./data/{}/ldi.json", metro.0),
            serde_json::to_string(&ldi).unwrap(),
        )
        .unwrap();
    });
}

fn correlation() {}

fn main() {
    let stages = [split, filter_languages, filter_puma, correlation];

    stages.iter().for_each(|x| x());
}
