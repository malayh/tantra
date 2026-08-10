module.exports = {
  sarathi: {
    input: "./openapi.json",
    output: {
      mode: "tags-split",
      target: "src/generated/api/",
      schemas: "src/generated/models",
      client: "react-query",
      httpClient: "axios",
      mock: false,
      override: {
        mutator: {
          path: "src/lib/apiClient.ts",
          name: "customInstance",
        },
      },
    },
  },
};
