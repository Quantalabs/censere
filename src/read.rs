#[derive(Clone)]
struct Variable {
    key: Vec<(usize, String)>,
    name: String,
}

type Entry = (Vec<usize>, Variable);

struct Data {
    values: Vec<Entry>,
}

fn filter<T: Fn(&Entry) -> bool>(x: &Data, func: T) -> Data {
    Data {
        values: &(x.clone().values).iter().filter(func).collect(),
    }
}
