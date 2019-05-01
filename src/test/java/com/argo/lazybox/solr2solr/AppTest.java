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
import java.net.URISyntaxException;

import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AppTest {
	@Test
	public void test() throws URISyntaxException, IOException {
		App.main(new String[] { //
				"-r", "10", // 10건씩
				"http://solr1:8983/solr/collection", //
				"http://solr2:48983/solr/collection" });
	}

}
