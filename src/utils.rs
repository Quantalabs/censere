pub fn find_gen(age: usize, year: usize) -> usize {
	let current = age + (2026 - year);

	current / 20
}
