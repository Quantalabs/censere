use crate::read;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::ops::{Add, AddAssign};
use std::{collections::HashMap, fs};

#[derive(Serialize, Deserialize, Clone)]
struct Agg {
    pop: usize,
    nonspeakers: f64,
    speakers: f64,
    migration: f64,
}

impl Add<Agg> for Agg {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        let pop = self.pop + other.pop;
        Self {
            pop,
            nonspeakers: ((self.nonspeakers * (self.pop as f64))
                + (other.nonspeakers * (other.pop as f64)))
                / (pop as f64),
            speakers: ((self.speakers * (self.pop as f64)) + (other.speakers * (other.pop as f64)))
                / (pop as f64),
            migration: ((self.migration * (self.pop as f64))
                + (other.migration * (other.pop as f64)))
                / (pop as f64),
        }
    }
}

impl Add<(Language, Vec<String>)> for Agg {
    type Output = Self;

    fn add(self, other: (Language, Vec<String>)) -> Self {
        let pop = self.pop + 1;
        let nonspeakers = (self.nonspeakers * (self.pop as f64)
            + if other.1[10] != other.0 { 1.0 } else { 0.0 })
            / (pop as f64);
        let speakers = (self.speakers * (self.pop as f64)
            + if other.1[10] != other.0 { 0.0 } else { 1.0 })
            / (pop as f64);
        let migration = (self.migration * (self.pop as f64)
            + if other.1[0] != other.1[9] { 0.0 } else { 1.0 })
            / (pop as f64);

        Self {
            pop,
            nonspeakers,
            speakers,
            migration,
        }
    }
}

impl AddAssign for Agg {
    fn add_assign(&mut self, rhs: Self) {
        let new = rhs + self.clone();

        self.pop = new.pop;
        self.speakers = new.speakers;
        self.nonspeakers = new.nonspeakers;
        self.migration = new.migration;
    }
}

type Metro = String;
type Puma = String;
type Year = usize;
type Generation = usize;
type Language = String;

type Record = Vec<String>;

#[derive(Serialize, Deserialize)]
struct PumaAgg {
    pop: usize,
    ldi: f64,
    l1: f64,
    languages: HashMap<Language, Agg>,
}

fn find_gen(age: usize, year: usize) -> usize {
    let current = age + (2026 - year);

    current / 20
}

pub fn filter_puma<T: Fn(Record) -> String>(metro: &str, num: &'static usize, classifier: T) {
    let records = read::read(
        format!("./data/raw/{}.csv", metro),
        (0..12).collect(),
        num,
        &0,
    )
    .unwrap()
    .0;

    let mut data: HashMap<
        Puma,
        HashMap<Year, HashMap<Generation, HashMap<Language, Vec<&Record>>>>,
    > = HashMap::new();

    records.iter().for_each(|x| {
        data.entry(x[1].clone())
            .or_default()
            .entry(str::parse(&x[0]).unwrap())
            .or_default()
            .entry(find_gen(
                str::parse(&x[3]).unwrap(),
                str::parse(&x[0]).unwrap(),
            ))
            .or_default()
            .entry(classifier(x.clone()))
            .or_default()
            .push(x);
    });

    data.iter().for_each(|(puma, years)| {
        years.iter().for_each(|(year, gens)| {
            let mut lang_aggs: HashMap<Language, Agg> = HashMap::new();
            let mut pop = 0;

            gens.iter().for_each(|(generation, langs)| {
                langs.iter().for_each(|(lang, records)| {
                    let agg;
                    if lang != "other" {
                        agg = records.iter().fold(
                            Agg {
                                pop: 0,
                                nonspeakers: 0.0,
                                speakers: 0.0,
                                migration: 0.0,
                            },
                            |acc: Agg, &x| acc + (lang.clone(), x.clone()),
                        );
                    } else {
                        agg = records.iter().fold(
                            Agg {
                                pop: 0,
                                nonspeakers: 0.0,
                                speakers: 0.0,
                                migration: 0.0,
                            },
                            |acc: Agg, &x| acc + (lang.clone(), x.clone()),
                        );
                    }

                    lang_aggs
                        .entry(lang.clone())
                        .and_modify(|x| *x += agg.clone())
                        .or_insert(agg.clone());

                    pop += 1;

                    fs::write(
                        format!(
                            "data/{}/{}/{}/{}/{}.json",
                            metro, puma, year, generation, lang
                        ),
                        serde_json::to_string(&agg).unwrap(),
                    );
                });
            });

            let ldi = 1.0
                - lang_aggs.iter().fold(0.0, |acc, (_, x)| {
                    acc + (x.speakers * (x.pop as f64) / (pop as f64)).powf(2.0)
                });

            let l1 = lang_aggs
                .iter()
                .fold(0.0, |acc, (_, x)| acc + (x.speakers * (x.pop as f64)))
                / (pop as f64);

            fs::write(
                format!("data/{}/{}/{}/agg.json", metro, puma, year),
                serde_json::to_string(&PumaAgg {
                    pop,
                    ldi,
                    l1,
                    languages: lang_aggs,
                })
                .unwrap(),
            );
        })
    });
}
