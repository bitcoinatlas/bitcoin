import { sync } from "@purifyjs/core";

const hash = sync<string>((set) => {
	set(location.hash);
	const interval = setInterval(() => set(location.hash), 100);
	return () => clearInterval(interval);
}).derive((hash) => hash.slice(1) || undefined);
