import axios, { AxiosError, AxiosRequestConfig } from "axios";
import { env } from "next-runtime-env";

export const apiClient = axios.create({
  baseURL: env("NEXT_PUBLIC_API_URL"),
  timeout: 30000,
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
