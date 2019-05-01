/*
Copyright 2019 argonet.co.kr

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */
package com.argo.lazybox.solr2solr;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.EntityBuilder;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.noggit.JSONUtil;
import org.noggit.ObjectBuilder;

/**
 *
 */
public class App {
	static final Logger log = Logger.getGlobal();

	protected static Options getOptions() {
		Options options = new Options();
		options.addOption(Option.builder("r").hasArg().longOpt("rows") //
				.desc("페이지당 요청 데이터 건수").build());
		options.addOption(Option.builder("c").hasArg().longOpt("cursor")//
				.desc("cursorMark. 기존 작업을 이어서 실행할 때").build());
		options.addOption(Option.builder("commitWithin").hasArg() //
				.desc("commitWithin. 기본값은 10000").build());
		options.addOption(Option.builder("q").hasArg().longOpt("query") //
				.desc("q 파라미터. 기본값은 *:*").build());
		options.addOption(Option.builder("u").hasArg().longOpt("uniqueKey") //
				.desc("<uniqueKey> 필드. 기본값은 'id'").build());

		return options;
	}

	public static void main(String[] args) {
		Options options = getOptions();
		CommandLineParser parser = new DefaultParser();
		try {
			CommandLine commands = parser.parse(options, args);

			solr2solr(commands);
		} catch (IllegalArgumentException | ParseException e) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("java -jar app.jar [OPTIONS] source-solr-url target-solr-url", options);
		} catch (URISyntaxException | IOException e) {
			System.out.println(ExceptionUtils.getMessage(e));
		}
	}

	protected static void solr2solr(CommandLine commands) throws URISyntaxException, IOException {
		List<String> args = commands.getArgList();
		String solr1 = args.size() > 0 ? args.get(0) : null;
		String solr2 = args.size() > 1 ? args.get(1) : null;

		if (StringUtils.isBlank(solr1) || StringUtils.isBlank(solr2)) {
			throw new IllegalArgumentException();
		}

		int rows = Integer.parseUnsignedInt(commands.getOptionValue("r", "1000"), 10);
		long commitWithin = Long.parseUnsignedLong(commands.getOptionValue("commitWithin", "10000"), 10);
		String cursor = commands.getOptionValue("c", "*");
		String query = commands.getOptionValue("q", "*:*");
		String uniqueKey = commands.getOptionValue("u", "id");

		pump(solr1, solr2, rows, commitWithin, cursor, query, uniqueKey);
	}

	private static void pump(String solr1, String solr2, int rows, long commitWithin, String cursor, String query,
			String uniqueKey) throws URISyntaxException, IOException {
		long total = 0;
		try (CloseableHttpClient http = HttpClientBuilder.create().build()) {
			for (;;) {
				Map<String, Object> content = get(http, solr1, query, uniqueKey, rows, cursor);
				// cursor = getCursorMark(content);
				cursor = (String) content.get("nextCursorMark");
				List<?> docs = docs(content);
				if (docs.isEmpty())
					break;

				post(http, solr2, docs, commitWithin);
				total += docs.size();
				log.info("Pump: " + total + ", nextCursorMark: " + cursor);
			}
		}
	}

	/**
	 * _version_필드 제거
	 * 
	 * @param object
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private static List<?> docs(Object content) {
		Map<String, Object> response = (Map<String, Object>) ((Map<String, Object>) content).get("response");
		Object object = response.get("docs");
		List<Object> docs = new ArrayList<>();
		if (object instanceof List) {
			for (Object obj : (List<Object>) object) {
				Map<String, Object> doc = (Map<String, Object>) obj;
				doc.remove("_version_");
				docs.add(doc);
			}
			return docs;
		} else {
			return null;
		}
	}

	/**
	 * @param http
	 * @param url
	 * @param query
	 * @param uniqueKey
	 * @param max
	 * @param cursor
	 * @return
	 * @throws URISyntaxException
	 * @throws IOException
	 */
	private static Map<String, Object> get(HttpClient http, String url, String query, String uniqueKey, int max,
			String cursor) throws URISyntaxException, IOException {
		URI uri = new URIBuilder(url + "/select") //
				.addParameter("q", query) //
				.addParameter("sort", uniqueKey + " asc") //
				.addParameter("wt", "json") //
				.addParameter("rows", String.valueOf(max)) //
				.addParameter("cursorMark", cursor).build();
		long start = System.currentTimeMillis();
		try {
			HttpGet get = new HttpGet(uri);
			HttpResponse response = http.execute(get);
			if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
				log.severe(EntityUtils.toString(response.getEntity()));
				throw new IllegalStateException(response.getStatusLine().toString());
			}

			String body = EntityUtils.toString(response.getEntity());
			Object obj = ObjectBuilder.fromJSON(body);
			// 결과는 Map 이어야 함
			if (!(obj instanceof Map))
				throw new IllegalStateException(obj.getClass().getName());

			return (Map<String, Object>) obj;
		} finally {
			log.info("GET " + uri + " " + (System.currentTimeMillis() - start) + " ms");
		}
	}

	/**
	 * @param http
	 * @param url
	 * @param content
	 * @param commitWithin
	 * @throws URISyntaxException
	 * @throws ClientProtocolException
	 * @throws IOException
	 */
	private static void post(CloseableHttpClient http, String url, List<?> content, long commitWithin)
			throws URISyntaxException, IOException {
		URI uri = new URIBuilder(url + "/update/json") //
				.addParameter("commitWithin", String.valueOf(commitWithin)).build(); //
		long start = System.currentTimeMillis();
		try {
			HttpPost post = new HttpPost(uri);
//		post.addHeader(HTTP.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString());
			String json = JSONUtil.toJSON(content);
			HttpEntity entity = EntityBuilder.create() //
					.setContentType(ContentType.APPLICATION_JSON).setText(json).build();
			post.setEntity(entity);

			HttpResponse response = http.execute(post);
			if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
				log.severe(EntityUtils.toString(response.getEntity()));
				throw new IllegalStateException(response.getStatusLine().toString());
			} else {
				EntityUtils.consumeQuietly(response.getEntity());
			}
		} finally {
			log.info("POST " + uri + " " + (System.currentTimeMillis() - start) + " ms");
		}
	}

}
