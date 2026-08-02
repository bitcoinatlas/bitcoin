function isRelativeSpecifier(specifier: string): boolean {
	return specifier.startsWith("./") || specifier.startsWith("../");
}

function checkStringLiteral(
	node: Deno.lint.StringLiteral | null,
	context: Deno.lint.RuleContext,
): void {
	if (!node) return;
	if (!isRelativeSpecifier(node.value)) return;

	context.report({
		node,
		message: `relative import '${node.value}' is not allowed, use an absolute specifier or import-map alias instead`,
	});
}

function checkExpression(
	node: Deno.lint.Expression,
	context: Deno.lint.RuleContext,
): void {
	if (node.type === "Literal" && typeof node.value === "string") {
		checkStringLiteral(node, context);
	} else if (
		node.type === "TemplateLiteral" &&
		node.expressions.length === 0 &&
		node.quasis[0] &&
		isRelativeSpecifier(node.quasis[0].cooked)
	) {
		context.report({
			node,
			message: "relative dynamic import is not allowed, use an absolute specifier or import-map alias instead",
		});
	}
}

const plugin: Deno.lint.Plugin = {
	name: "no-relative-imports",
	rules: {
		"no-relative-imports": {
			create(context) {
				return {
					// import foo from "./foo.ts"
					ImportDeclaration(node: Deno.lint.ImportDeclaration): void {
						checkStringLiteral(node.source, context);
					},

					// export { foo } from "./foo.ts"
					ExportNamedDeclaration(node: Deno.lint.ExportNamedDeclaration): void {
						checkStringLiteral(node.source, context);
					},

					// export * from "./foo.ts"
					ExportAllDeclaration(node: Deno.lint.ExportAllDeclaration): void {
						checkStringLiteral(node.source, context);
					},

					// await import("./foo.ts") or import(`./foo.ts`)
					ImportExpression(node: Deno.lint.ImportExpression): void {
						checkExpression(node.source, context);
					},

					// import foo = require("./foo.ts")
					TSImportEqualsDeclaration(node: Deno.lint.TSImportEqualsDeclaration): void {
						if (node.moduleReference.type === "TSExternalModuleReference") {
							checkStringLiteral(node.moduleReference.expression, context);
						}
					},
				};
			},
		},
	},
};

export default plugin;
