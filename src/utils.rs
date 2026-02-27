use crate::read;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::ops::{Add, AddAssign};
use std::{collections::HashMap, fs};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Agg {
    pub pop: usize,
    pub nonspeakers: f64,
    pub speakers: f64,
    pub migration: HashMap<usize, usize>,
    pub langs: Vec<String>,
    pub ancestries: Vec<String>,
}

impl Add<Agg> for Agg {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        let pop = self.pop + other.pop;
        let mut langs = self.langs;
        let mut ancestries = self.ancestries;

        other.langs.iter().for_each(|x| {
            if !langs.contains(x) {
                langs.push(x.clone());
            }
        });
        other.ancestries.iter().for_each(|x| {
            if !ancestries.contains(x) {
                ancestries.push(x.clone());
            }
        });

        Self {
            pop,
            nonspeakers: ((self.nonspeakers * (self.pop as f64))
                + (other.nonspeakers * (other.pop as f64)))
                / (pop as f64),
            speakers: ((self.speakers * (self.pop as f64)) + (other.speakers * (other.pop as f64)))
                / (pop as f64),
            migration: self
                .migration
                .iter()
                .map(|(k, v)| (k.clone(), v + other.migration.get(k).unwrap_or_else(|| &0)))
                .collect(),
            langs,
            ancestries,
        }
    }
}

impl Add<Vec<String>> for Agg {
    type Output = Self;

    fn add(self, other: Vec<String>) -> Self {
        let pop = self.pop + 1;
        let nonspeakers = (self.nonspeakers * (self.pop as f64)
            + if self.langs.contains(&other[10]) {
                1.0
            } else {
                0.0
            })
            / (pop as f64);
        let speakers = (self.speakers * (self.pop as f64)
            + if self.langs.contains(&other[10]) {
                0.0
            } else {
                1.0
            })
            / (pop as f64);
        let mut migration = self.migration.clone();

        migration
            .entry(str::parse(other[9].as_str()).unwrap())
            .and_modify(|e| *e += 1)
            .or_insert(1);

        Self {
            pop,
            nonspeakers,
            speakers,
            migration,
            langs: self.langs,
            ancestries: self.ancestries,
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
        self.langs = new.langs;
        self.ancestries = new.ancestries;
    }
}

type Metro = String;
type Puma = String;
type Year = usize;
type Generation = usize;
type Language = String;

type Record = Vec<String>;

#[derive(Serialize, Deserialize)]
pub struct PumaAgg {
    pub pop: usize,
    pub ldi: f64,
    pub languages: HashMap<Language, Agg>,
}

fn find_gen(age: usize, year: usize) -> usize {
    let current = age + (2026 - year);

    current / 20
}

pub fn filter_puma(metro: &str, num: &'static usize, filters: HashMap<String, &read::Codes>) {
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
            .entry({
                let mut ret = String::from("other");

                filters.iter().for_each(|filter| {
                    if filter.1.ancestry.contains(&x[5]) || filter.1.ancestry.contains(&x[7]) {
                        ret = filter.0.clone();
                    } else if filter.1.languages.contains(&x[10]) {
                        ret = filter.0.clone();
                    }
                });

                ret
            })
            .or_default()
            .push(x);
    });

    let _ = fs::create_dir(format!("data/{}", metro));

    let mut metro_agg: HashMap<Language, Agg> = HashMap::new();
    let mut metro_gen_agg: HashMap<String, Agg> = HashMap::new();

    data.iter().for_each(|(puma, years)| {
        let _ = fs::create_dir(format!("data/{}/{}", metro, puma));
        years.iter().for_each(|(year, gens)| {
            let mut loc_aggs: HashMap<Language, Agg> = HashMap::new();
            let mut gen_aggs: HashMap<String, Agg> = HashMap::new();
            let mut other_loc: HashMap<Language, usize> = HashMap::new();
            let mut pop = 0;

            gens.iter().for_each(|(generation, langs)| {
                langs.iter().for_each(|(lang, records)| {
                    if lang != "other" {
                        let agg = records.iter().fold(
                            Agg {
                                pop: 0,
                                nonspeakers: 0.0,
                                speakers: 0.0,
                                migration: HashMap::new(),
                                langs: filters.get(lang).unwrap().languages.clone(),
                                ancestries: filters.get(lang).unwrap().ancestry.clone(),
                            },
                            |acc: Agg, &x| {
                                pop += 1;

                                if x[10] != *lang {
                                    other_loc
                                        .entry(x[10].clone())
                                        .and_modify(|x| *x += 1)
                                        .or_insert(1);
                                }
                                acc + x.clone()
                            },
                        );

                        loc_aggs
                            .entry(lang.clone())
                            .and_modify(|x| *x += agg.clone())
                            .or_insert(agg.clone());

                        gen_aggs
                            .entry(format!("{}_{}", generation, lang))
                            .and_modify(|x| *x += agg.clone())
                            .or_insert(agg.clone());

                        metro_agg
                            .entry(format!("{}_{}", year, lang))
                            .and_modify(|x| *x += agg.clone())
                            .or_insert(agg.clone());

                        metro_gen_agg
                            .entry(format!("{}_{}_{}", year, generation, lang))
                            .and_modify(|x| *x += agg.clone())
                            .or_insert(agg.clone());

                        let _ = fs::write(
                            format!(
                                "data/{}/{}/{}_{}_{}.json",
                                metro, puma, year, generation, lang
                            ),
                            serde_json::to_string(&agg).unwrap(),
                        )
                        .unwrap();
                    } else {
                        other_loc
                            .entry(lang.clone())
                            .and_modify(|x| *x += 1)
                            .or_insert(1);
                    }
                });
            });

            let ldi =
                1.0 - loc_aggs.iter().fold(0.0, |acc, (_, x)| {
                    acc + ((x.speakers * (x.pop as f64)) / (pop as f64)).powf(2.0)
                }) - other_loc.iter().fold(0.0, |acc, (_, x)| {
                    acc + ((*x as f64) / (pop as f64)).powf(2.0)
                });

            let _ = fs::write(
                format!("data/{}/{}/{}_gen_agg.json", metro, puma, year),
                serde_json::to_string(&gen_aggs).unwrap(),
            )
            .unwrap();

            let _ = fs::write(
                format!("data/{}/{}/{}_agg.json", metro, puma, year),
                serde_json::to_string(&PumaAgg {
                    pop,
                    ldi,
                    languages: loc_aggs,
                })
                .unwrap(),
            )
            .unwrap();
        });
    });

    let _ = fs::write(
        format!("data/{}/agg.json", metro),
        serde_json::to_string(&metro_agg).unwrap(),
    )
    .unwrap();

    let _ = fs::write(
        format!("data/{}/gen_agg.json", metro),
        serde_json::to_string(&metro_gen_agg).unwrap(),
    )
    .unwrap();
}
