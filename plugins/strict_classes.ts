type ClassMember = Deno.lint.MethodDefinition | Deno.lint.PropertyDefinition;
type AnyClassMember =
	| Deno.lint.MethodDefinition
	| Deno.lint.PropertyDefinition
	| Deno.lint.TSAbstractMethodDefinition
	| Deno.lint.TSAbstractPropertyDefinition;

function getAllClassDeclarations(program: Deno.lint.Program): Deno.lint.ClassDeclaration[] {
	const classes: Deno.lint.ClassDeclaration[] = [];
	for (const stmt of program.body) {
		if (stmt.type === "ClassDeclaration") {
			classes.push(stmt);
		} else if (stmt.type === "ExportNamedDeclaration" && stmt.declaration?.type === "ClassDeclaration") {
			classes.push(stmt.declaration);
		} else if (stmt.type === "ExportDefaultDeclaration" && stmt.declaration.type === "ClassDeclaration") {
			classes.push(stmt.declaration);
		}
	}
	return classes;
}

function findBaseMember(
	node: ClassMember,
	context: Deno.lint.RuleContext,
): AnyClassMember | null {
	const sourceCode = context.sourceCode;

	const classBody = node.parent;
	if (classBody.type !== "ClassBody") return null;

	const classDecl = classBody.parent;
	if (classDecl.type !== "ClassDeclaration") return null;
	if (!classDecl.superClass) return null;

	const superName = classDecl.superClass.type === "Identifier" ? classDecl.superClass.name : null;
	if (!superName) return null;

	const allClasses = getAllClassDeclarations(sourceCode.ast);
	const baseDecl = allClasses.find(
		(c) => c.id !== null && c.id.type === "Identifier" && c.id.name === superName,
	);

	if (!baseDecl) return null;

	const keyName = node.key.type === "Identifier" ? node.key.name : null;
	if (!keyName) return null;

	return (
		baseDecl.body.body.find(
			(m): m is AnyClassMember =>
				(m.type === "MethodDefinition" ||
					m.type === "PropertyDefinition" ||
					m.type === "TSAbstractMethodDefinition" ||
					m.type === "TSAbstractPropertyDefinition") &&
				m.key.type === "Identifier" &&
				m.key.name === keyName,
		) ?? null
	);
}

function findBaseClass(
	context: Deno.lint.RuleContext,
): (classDecl: Deno.lint.ClassDeclaration) => Deno.lint.ClassDeclaration | null {
	const sourceCode = context.sourceCode;
	const allClasses = getAllClassDeclarations(sourceCode.ast);

	return (classDecl: Deno.lint.ClassDeclaration): Deno.lint.ClassDeclaration | null => {
		if (!classDecl.superClass) return null;

		const superName = classDecl.superClass.type === "Identifier" ? classDecl.superClass.name : null;
		if (!superName) return null;

		return allClasses.find(
			(c) => c.id !== null && c.id.type === "Identifier" && c.id.name === superName,
		) ?? null;
	};
}

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
							// message: "class members must explicitly use public / protected / private or use #private",
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
		"explicit-override": {
			create(context) {
				function checkRequireOverride(node: ClassMember): void {
					const base = findBaseMember(node, context);
					if (!base) return;

					if (!node.override) {
						context.report({
							node,
							message: "overridden members must use the 'override' keyword",
						});
					}
				}

				return {
					MethodDefinition(node: Deno.lint.MethodDefinition): void {
						if (node.kind === "constructor") return;
						checkRequireOverride(node);
					},

					PropertyDefinition(node: Deno.lint.PropertyDefinition): void {
						checkRequireOverride(node);
					},
				};
			},
		},
		"consistent-modifiers": {
			create(context) {
				function checkModifierChange(node: ClassMember): void {
					const base = findBaseMember(node, context);
					if (!base) return;

					const baseAccessibility = base.accessibility ?? "public";
					const nodeAccessibility = node.accessibility ?? "public";

					if (baseAccessibility !== nodeAccessibility) {
						context.report({
							node,
							message: `override cannot change accessibility: base is ${baseAccessibility}, override is ${nodeAccessibility}`,
						});
					}

					const baseReadonly = "readonly" in base ? base.readonly : false;
					const nodeReadonly = node.readonly;
					if (baseReadonly !== nodeReadonly) {
						context.report({
							node,
							message: `override cannot change readonly modifier`,
						});
					}

					if (base.static !== node.static) {
						context.report({
							node,
							message: `override cannot change static modifier`,
						});
					}
				}

				return {
					MethodDefinition(node: Deno.lint.MethodDefinition): void {
						if (node.kind === "constructor") return;
						checkModifierChange(node);
					},
					PropertyDefinition(node: Deno.lint.PropertyDefinition): void {
						checkModifierChange(node);
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
				const getBaseClass = findBaseClass(context);

				function getAbstractStatics(
					baseDecl: Deno.lint.ClassDeclaration | null,
				): Map<string, ClassMember> {
					const abstractStatics = new Map<string, ClassMember>();
					if (!baseDecl?.abstract) return abstractStatics;

					for (const member of baseDecl.body.body) {
						if (member.type !== "MethodDefinition" && member.type !== "PropertyDefinition") continue;
						if (!member.static) continue;
						const keyName = member.key.type === "Identifier" ? member.key.name : null;
						if (!keyName) continue;

						if (member.type === "MethodDefinition" && isEmptyMethodBody(member)) {
							abstractStatics.set(keyName, member);
						} else if (member.type === "PropertyDefinition" && isUnassignedProperty(member)) {
							abstractStatics.set(keyName, member);
						}
					}

					return abstractStatics;
				}

				return {
					ClassDeclaration(node: Deno.lint.ClassDeclaration): void {
						if (!node.abstract) {
							for (const member of node.body.body) {
								if (member.type !== "MethodDefinition") continue;
								if (!member.static) continue;
								if (!isEmptyMethodBody(member)) continue;

								context.report({
									node: member,
									message: "static methods cannot have empty bodies in non-abstract classes",
								});
							}
						}

						if (node.abstract) return;

						const baseDecl = getBaseClass(node);
						if (!baseDecl) return;

						const abstractStatics = getAbstractStatics(baseDecl);
						if (abstractStatics.size === 0) return;

						const overriddenStatics = new Set<string>();
						for (const member of node.body.body) {
							if (member.type !== "MethodDefinition" && member.type !== "PropertyDefinition") continue;
							if (!member.static) continue;
							const keyName = member.key.type === "Identifier" ? member.key.name : null;
							if (keyName) overriddenStatics.add(keyName);
						}

						for (const [name, baseMember] of abstractStatics) {
							if (!overriddenStatics.has(name)) {
								const kind = baseMember.type === "MethodDefinition" ? "method" : "property";
								context.report({
									node,
									message: `class must override abstract static ${kind} '${name}' from base class`,
								});
							}
						}
					},
				};
			},
		},
	},
};

export default plugin;
