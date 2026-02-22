use crate::read;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs};

#[derive(Serialize, Deserialize, Clone)]
struct Agg {
    pop: usize,
    lang: usize,
    mig: HashMap<String, usize>,
}

fn find_gen(age: usize, year: usize) -> usize {
    let current = age + (2026 - year);

    current / 20
}

pub fn filter_puma(metro: &str, lang: &str) {
    let (all, header) =
        read::read_single(format!("data/{}/{}.csv", metro, lang), (0..11).collect()).unwrap();
    let mut pumas: HashMap<String, HashMap<usize, Vec<&Vec<String>>>> = HashMap::new();

    all.iter().for_each(|record| {
        pumas
            .entry(record[1].clone())
            .and_modify(|x| {
                x.entry(record[0].parse::<usize>().unwrap())
                    .and_modify(|y| y.push(record))
                    .or_insert(vec![record]);
            })
            .or_insert({
                let x = HashMap::new();
                x.insert(record[0].parse::<usize>().unwrap(), vec![record]);
                x
            });
    });

    let mut ags_loc: Vec<Agg> = vec![];
    let mut ags_gen: HashMap<String, Vec<Agg>> = HashMap::new();

    pumas.iter().for_each(|(loc, years)| {
        years.iter().for_each(|(year, records)| {
            let mut generations: HashMap<String, Vec<&Vec<String>>> = HashMap::new();
            let mut aggs: Vec<Agg> = vec![];

            records.iter().for_each(|record| {
                generations
                    .entry(
                        find_gen(
                            record[3].parse::<usize>().unwrap(),
                            record[0].parse::<usize>().unwrap(),
                        )
                        .to_string(),
                    )
                    .and_modify(|x| x.push(record))
                    .or_insert(vec![record]);
            });

            generations.iter().for_each(|(generation, recs)| {
                let _ = fs::create_dir(format!("data/{}/{}", metro, loc));
                let _ = read::export(
                    format!("data/{}/{}/{}_{}_gen{}.csv", metro, loc, year, lang, generation),
                    recs.clone(),
                    &header,
                );

                let agg = recs.iter().fold(
                    Agg {
                        pop: 0,
                        lang: 0,
                        mig: HashMap::new(),
                    },
                    |acc, &x| Agg {
                        pop: acc.pop + 1,
                        lang: if ![String::from("00"), String::from("01")].contains(&x[10]) {
                            acc.lang + 1
                        } else {
                            acc.lang
                        },
                        mig: if "0000" != &x[9] {
                            let mut new = acc.mig.clone();
                            new.entry(x[9].clone()).and_modify(|y| *y += 1).or_insert(1);
                            new
                        } else {
                            acc.mig
                        },
                    },
                );

                let serialized = serde_json::to_string(&agg).unwrap();

                fs::create_dir(format!("data/{}/{}/{}", metro, loc, year));
                fs::write(
                    format!("data/{}/{}/{}/{}_gen{}.json", metro, loc, year, lang, generation),
                    serialized,
                )
                .unwrap();

                ags_gen
                    .entry(generation.clone())
                    .and_modify(|x| x.push(agg.clone()))
                    .or_insert(vec![agg.clone()]);

                aggs.push(agg.clone());
            });

            let agg = aggs.iter().fold(
                Agg {
                    pop: 0,
                    lang: 0,
                    mig: HashMap::new(),
                },
                |acc, g| Agg {
                    pop: acc.pop + g.pop,
                    lang: acc.lang + g.lang,
                    mig: acc.mig.into_iter().fold(g.mig.clone(), |acc, (k, v)| {
                        let mut new = acc.clone();
                        new.entry(k).and_modify(|x| *x += v).or_insert(v);
                        new
                    }),
                },
            );

            let serialized = serde_json::to_string(&agg).unwrap();

            fs::write(
                format!("data/{}/{}/{}_all.json", metro, loc, lang),
                serialized,
            )
            .unwrap();

            ags_loc.push(agg);
        });
    });

    let ag_gen: HashMap<String, Agg> = ags_gen
        .iter()
        .map(|(k, v)| {
            (
                k.clone(),
                v.iter().fold(
                    Agg {
                        pop: 0,
                        lang: 0,
                        mig: HashMap::new(),
                    },
                    |acc, x| Agg {
                        pop: acc.pop + x.pop,
                        lang: acc.lang + x.lang,
                        mig: acc.mig.into_iter().fold(x.mig.clone(), |acc, (k, v)| {
                            let mut new = acc.clone();
                            new.entry(k).and_modify(|x| *x += v).or_insert(v);
                            new
                        }),
                    },
                ),
            )
        })
        .collect();
    let ag_loc = ags_loc.iter().fold(
        Agg {
            pop: 0,
            lang: 0,
            mig: HashMap::new(),
        },
        |acc, x| Agg {
            pop: acc.pop + x.pop,
            lang: acc.lang + x.lang,
            mig: acc.mig.into_iter().fold(x.mig.clone(), |acc, (k, v)| {
                let mut new = acc.clone();
                new.entry(k).and_modify(|x| *x += v).or_insert(v);
                new
            }),
        },
    );

    fs::write(
        format!("data/{}/{}_all.json", metro, lang),
        serde_json::to_string(&ag_loc).unwrap(),
    )
    .unwrap();

    ag_gen.iter().for_each(|(k, v)| {
        fs::write(
            format!("data/{}/{}_gen{}.json", metro, lang, k),
            serde_json::to_string(v).unwrap(),
        )
        .unwrap();
    });
}
