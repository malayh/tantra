import axios from "axios";
import NextAuth, { type NextAuthOptions } from "next-auth";
import CredentialsProvider from "next-auth/providers/credentials";
import { env } from "next-runtime-env";

const MAX_AGE = 24 * 60 * 60;

const apiUrl = () => process.env.API_URL_INTERNAL || env("NEXT_PUBLIC_API_URL") || "http://localhost:8000";

const expiryOf = (accessToken: string): number => {
  const payload = accessToken.split(".")[1];
  const { exp } = JSON.parse(Buffer.from(payload, "base64url").toString("utf8")) as { exp: number };
  return exp * 1000;
};

const authOptions: NextAuthOptions = {
  providers: [
    CredentialsProvider({
      name: "credentials",
      credentials: {
        email: { label: "Email", type: "email" },
        password: { label: "Password", type: "password" },
      },
      async authorize(credentials) {
        if (!credentials?.email || !credentials?.password) return null;
        try {
          const { data } = await axios.post<{ access_token: string }>(`${apiUrl()}/api/auth/login`, {
            email: credentials.email,
            password: credentials.password,
          });
          return {
            id: credentials.email,
            email: credentials.email,
            accessToken: data.access_token,
            tokenExpiry: expiryOf(data.access_token),
          };
        } catch {
          return null;
        }
      },
    }),
  ],
  session: { strategy: "jwt", maxAge: MAX_AGE },
  pages: { signIn: "/login" },
  callbacks: {
    async jwt({ token, user }) {
      if (user) {
        token.accessToken = user.accessToken;
        token.tokenExpiry = user.tokenExpiry;
      }
      return token;
    },
    async session({ session, token }) {
      session.accessToken = token.accessToken;
      session.tokenExpiry = token.tokenExpiry;
      return session;
    },
  },
};

const handler = NextAuth(authOptions);

export { handler as GET, handler as POST };
