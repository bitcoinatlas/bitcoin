export function html(...params: Parameters<typeof String.raw>): DocumentFragment {
	const raw = String.raw(...params);

	const template = document.createElement("template");
	template.innerHTML = raw;

	return template.content;
}
