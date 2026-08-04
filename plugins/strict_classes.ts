type ClassMember =
	| Deno.lint.MethodDefinition
	| Deno.lint.PropertyDefinition
	| Deno.lint.TSAbstractMethodDefinition
	| Deno.lint.TSAbstractPropertyDefinition;

const plugin: Deno.lint.Plugin = {
	name: "strict-classes",
	rules: {
		"explicit-accessibility": {
			create(context) {
				function check(node: ClassMember): void {
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
						check(node);
					},
					PropertyDefinition(node: Deno.lint.PropertyDefinition): void {
						check(node);
					},
					TSAbstractMethodDefinition(node): void {
						check(node);
					},
					TSAbstractPropertyDefinition(node): void {
						check(node);
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
							if (member.value !== null) continue;

							context.report({
								node: member,
								message: "static properties must be initialized in non-abstract classes",
							});
						}
					},
				};
			},
		},
	},
};

export default plugin;
