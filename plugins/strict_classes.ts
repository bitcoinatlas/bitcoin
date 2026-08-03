type ClassMember = Deno.lint.MethodDefinition | Deno.lint.PropertyDefinition;

function isEmptyMethodBody(node: Deno.lint.MethodDefinition): boolean {
	const value = node.value;
	if (value.type !== "FunctionExpression") return false;
	const body = value.body;
	return body.body.length === 0;
}

function isUnassignedProperty(node: Deno.lint.PropertyDefinition): boolean {
	return node.value === null;
}

const plugin: Deno.lint.Plugin = {
	name: "strict-classes",
	rules: {
		"explicit-accessibility": {
			create(context) {
				function checkExplicitAccessibility(node: ClassMember): void {
					if (node.key.type === "PrivateIdentifier") {
						context.report({
							node,
							message: "class members must explicitly use public, protected or private, can't use #private",
						});
					}

					if (!node.accessibility) {
						context.report({
							node,
							message: "class members must explicitly use public, protected or private",
						});
					}
				}

				return {
					MethodDefinition(node: Deno.lint.MethodDefinition): void {
						checkExplicitAccessibility(node);
					},

					PropertyDefinition(node: Deno.lint.PropertyDefinition): void {
						checkExplicitAccessibility(node);
					},
				};
			},
		},
		"require-static-init": {
			create(context) {
				return {
					ClassDeclaration(node: Deno.lint.ClassDeclaration): void {
						if (node.abstract) return;

						for (const member of node.body.body) {
							if (member.type !== "PropertyDefinition") continue;
							if (!member.static) continue;
							if (!isUnassignedProperty(member)) continue;

							context.report({
								node: member,
								message: "static properties must be initialized in non-abstract classes",
							});
						}
					},
				};
			},
		},
		"abstract-static-methods": {
			create(context) {
				return {
					ClassDeclaration(node: Deno.lint.ClassDeclaration): void {
						if (node.abstract) return;

						for (const member of node.body.body) {
							if (member.type !== "MethodDefinition") continue;
							if (!member.static) continue;
							if (!isEmptyMethodBody(member)) continue;

							context.report({
								node: member,
								message: "static methods cannot have empty bodies in non-abstract classes",
							});
						}
					},
				};
			},
		},
	},
};

export default plugin;
