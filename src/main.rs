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
    });
}

fn correlation() {}

fn main() {
    let stages = [split, filter_languages, filter_puma, correlation];

    stages.iter().for_each(|x| x());
}
