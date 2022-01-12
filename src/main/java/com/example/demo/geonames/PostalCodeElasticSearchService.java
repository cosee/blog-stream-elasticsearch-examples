package com.example.demo.geonames;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.ClearScrollRequest;
import co.elastic.clients.elasticsearch.core.CountResponse;
import co.elastic.clients.elasticsearch.core.ScrollRequest;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.example.demo.geonames.model.PostalCode;
import com.example.demo.geonames.streamingscrollscan.ScrollScanIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Service
public class PostalCodeElasticSearchService {

    public static final String INDEX_NAME = "postalcodes";
    public static final Time KEEP_ALIVE = Time.of(t -> t.time("10m"));
    public static final int SCROLL_PAGE_SIZE = 1000;

    RestClient restClient = RestClient.builder(
            new HttpHost("localhost", 9200)).build();
    ElasticsearchTransport transport = new RestClientTransport(
            restClient, new JacksonJsonpMapper());
    ElasticsearchClient client = new ElasticsearchClient(transport);

    private final ObjectMapper objectMapper = new ObjectMapper();

    @SneakyThrows
    public void addPostalCodes(List<PostalCode> postalCodeList) {
        BulkResponse bulk = client.bulk(b -> b.index(INDEX_NAME)
                .operations(postalCodeList.stream()
                        .map(
                                postalCode -> BulkOperation.of(b1 -> b1
                                        .index(i -> i
                                                .id(postalCode.getId()).document(postalCode)
                                        )
                                )
                        )
                        .collect(Collectors.toList())));
        for (BulkResponseItem item : bulk.items()) {
            if (item.error() != null) {
                throw new IllegalStateException(item.error().reason());
            }
        }
    }

    @SneakyThrows
    public void refresh() {
        client.indices().refresh(b -> b.index(INDEX_NAME));
    }

    @SneakyThrows
    public List<PostalCode> fetchAllPostalCodes(String countryIso2) {
        SearchRequest request = buildSearchRequest(countryIso2);
        SearchResponse<PostalCode> search = client.search(request, PostalCode.class);
        return search.documents();
    }

    @SneakyThrows
    public List<PostalCode> fetchAllPostalCodesByScrollScan(String countryIso2) {
        SearchResponse<PostalCode> initialResponse = client.search(s ->
                        prepareSearchBuilder(s, countryIso2).scroll(KEEP_ALIVE),
                PostalCode.class);

        List<PostalCode> result = new LinkedList<>(initialResponse.documents());
        List<PostalCode> postalCodes;
        String scrollId = initialResponse.scrollId();
        try {
            SearchResponse<PostalCode> scrollResponse;
            do {
                scrollResponse = client.scroll(buildScrollRequest(scrollId), PostalCode.class);
                postalCodes = scrollResponse.documents();
                scrollId = scrollResponse.scrollId();
                result.addAll(postalCodes);
            } while (scrollResponse.documents().size() > 0);
        } finally {
            client.clearScroll(buildClearScrollRequest(scrollId));
        }

        return result;
    }

    private SearchRequest buildSearchRequest(String countryIso2) {
        return SearchRequest.of(b -> prepareSearchBuilder(b, countryIso2)
        );
    }

    private SearchRequest.Builder prepareSearchBuilder(SearchRequest.Builder builder, String countryIso2) {
        return builder
                .index(INDEX_NAME)
                .query(q -> q
                        .term(t -> t
                                .field("countryIso2.keyword")
                                .value(v -> v
                                        .stringValue(countryIso2)
                                )
                        )
                )
                .size(SCROLL_PAGE_SIZE);
    }

    private ScrollRequest buildScrollRequest(String scrollId) {
        return ScrollRequest.of(s -> s.scrollId(scrollId).scroll(KEEP_ALIVE));
    }

    private ClearScrollRequest buildClearScrollRequest(String scrollId) {
        return ClearScrollRequest.of(c->c.scrollId(scrollId));
    }

    public Stream<PostalCode> streamAllPostalCodesByScrollScan(String countryIso2) {
        SearchRequest searchRequest = buildSearchRequest(countryIso2);
        return streamSearchHits(searchRequest);
    }

    private Stream<PostalCode> streamSearchHits(SearchRequest searchRequest) {
        ScrollScanIterator<PostalCode> scrollScanIterator = ScrollScanIterator.of(client, searchRequest, PostalCode.class);
        Spliterator<SearchResponse<PostalCode>> spliterator = Spliterators.spliteratorUnknownSize(scrollScanIterator, 0);
        return StreamSupport.stream(spliterator, false)
                .onClose(scrollScanIterator::close)
                .flatMap(searchHits -> searchHits.documents().stream());
    }

    @SneakyThrows
    void deleteIndex() {
        client.indices().delete(d -> d.index(INDEX_NAME));
    }

    @SneakyThrows
    public long countAllPostalCodes() {
        CountResponse count = client.count(c -> c.index(INDEX_NAME).query(q->q.matchAll(m -> m)));
        return count.count();
    }

    @PreDestroy
    private void shutdown() throws IOException {
        restClient.close();
    }
}
