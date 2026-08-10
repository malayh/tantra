import axios, { AxiosError, AxiosRequestConfig } from "axios";
import { getSession } from "next-auth/react";
import { env } from "next-runtime-env";

export const apiClient = axios.create({
  baseURL: env("NEXT_PUBLIC_API_URL"),
  timeout: 30000,
});

let cachedToken: { token: string; expiry: number } | null = null;

export const getToken = async (): Promise<string | null> => {
  if (cachedToken && cachedToken.expiry > Date.now()) return cachedToken.token;

  cachedToken = null;
  const session = await getSession();
  if (!session?.accessToken || !session.tokenExpiry || session.tokenExpiry <= Date.now()) return null;

  cachedToken = { token: session.accessToken, expiry: session.tokenExpiry };
  return cachedToken.token;
};

export const clearToken = () => {
  cachedToken = null;
};

apiClient.interceptors.request.use(async (config) => {
  const token = await getToken();
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

export const customInstance = <T>(config: AxiosRequestConfig, options?: AxiosRequestConfig): Promise<T> => {
  const source = axios.CancelToken.source();
  const promise = apiClient({ ...config, ...options, cancelToken: source.token }).then(
    ({ data }) => data,
  ) as Promise<T> & { cancel?: () => void };

  promise.cancel = () => source.cancel("Query was cancelled");

  return promise;
};

export type ErrorType<E> = AxiosError<E>;

export type BodyType<B> = B;
