import type { DefaultSession } from "next-auth";

declare module "next-auth" {
  interface Session extends DefaultSession {
    accessToken?: string;
    tokenExpiry?: number;
  }

  interface User {
    accessToken?: string;
    tokenExpiry?: number;
  }
}

declare module "next-auth/jwt" {
  interface JWT {
    accessToken?: string;
    tokenExpiry?: number;
  }
}
