package lego.odata.service;

import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.olingo.commons.api.data.ContextURL;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.EntityCollection;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.ValueType;
import org.apache.olingo.commons.api.edm.EdmElement;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.api.ex.ODataRuntimeException;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.api.http.HttpHeader;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.ODataLibraryException;
import org.apache.olingo.server.api.ODataRequest;
import org.apache.olingo.server.api.ODataResponse;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.processor.EntityCollectionProcessor;
import org.apache.olingo.server.api.serializer.EntityCollectionSerializerOptions;
import org.apache.olingo.server.api.serializer.ODataSerializer;
import org.apache.olingo.server.api.serializer.SerializerException;
import org.apache.olingo.server.api.serializer.SerializerResult;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriResource;
import org.apache.olingo.server.api.uri.UriResourceEntitySet;
import org.apache.olingo.server.api.uri.queryoption.ExpandOption;
import org.apache.olingo.server.api.uri.queryoption.FilterOption;
import org.apache.olingo.server.api.uri.queryoption.OrderByOption;
import org.apache.olingo.server.api.uri.queryoption.SelectItem;
import org.apache.olingo.server.api.uri.queryoption.SelectOption;
import org.apache.olingo.server.api.uri.queryoption.SkipOption;
import org.apache.olingo.server.api.uri.queryoption.TopOption;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;

import static org.elasticsearch.node.NodeBuilder.*;

public class LegoEntityCollectionProcessor implements EntityCollectionProcessor {

	private OData odata;
	private ServiceMetadata serviceMetaData;
	private Client client;
	private String elasticIndex = "events";
	private String elasticClusterName = "legodata";
	
	@SuppressWarnings("resource")
	public void init(OData odata, ServiceMetadata serviceMetaData) {
		this.odata = odata;
		this.serviceMetaData = serviceMetaData;
		
		
		/*
		Node node = nodeBuilder()
					.settings(ImmutableSettings.settingsBuilder().put("discovery.zen.ping.multicast.enabled", false).put("http.enabled", false).build())
					.client(true)
					.clusterName(elasticClusterName)
					.node();
		*/			
		Settings settings = ImmutableSettings.settingsBuilder()
		        .put("cluster.name", elasticClusterName).build();
		this.client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress("127.0.0.1", 9300));
		
	}

	public void readEntityCollection(ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType responseFormat)
			throws ODataApplicationException, SerializerException  {
		// 1st we have retrieve the requested EntitySet from the uriInfo object (representation of the parsed service URI)
		  List<UriResource> resourcePaths = uriInfo.getUriResourceParts();
		  FilterOption filterOption = uriInfo.getFilterOption();
		  SelectOption selectOption = uriInfo.getSelectOption();
		  ExpandOption expandOption = uriInfo.getExpandOption();
		  OrderByOption orderByOption = uriInfo.getOrderByOption();
		  SkipOption skipOption = uriInfo.getSkipOption();
		  TopOption topOption = uriInfo.getTopOption();
		  UriResourceEntitySet uriResourceEntitySet = (UriResourceEntitySet) resourcePaths.get(0); // in our example, the first segment is the EntitySet
		  EdmEntitySet edmEntitySet = uriResourceEntitySet.getEntitySet();

		  // 2nd: fetch the data from backend for this requested EntitySetName
		  // it has to be delivered as EntitySet object
		  EntityCollection entitySet = readEntitySet(edmEntitySet, filterOption, selectOption, expandOption, orderByOption, skipOption, topOption);

		  // 3rd: create a serializer based on the requested format (json)
		  ODataSerializer serializer = odata.createSerializer(responseFormat);

		  // 4th: Now serialize the content: transform from the EntitySet object to InputStream
		  EdmEntityType edmEntityType = edmEntitySet.getEntityType();
		  ContextURL contextUrl = ContextURL.with().entitySet(edmEntitySet).build();

		  final String id = request.getRawBaseUri() + "/" + edmEntitySet.getName();
		  EntityCollectionSerializerOptions opts = EntityCollectionSerializerOptions.with().id(id).contextURL(contextUrl).build();
		  SerializerResult serializedContent = serializer.entityCollection(serviceMetaData, edmEntityType, entitySet, opts);

		  this.client.close();
		  
		  // Finally: configure the response object: set the body, headers and status code
		  response.setContent(serializedContent.getContent());
		  response.setStatusCode(HttpStatusCode.OK.getStatusCode());
		  response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
	}
	
	public EntityCollection readEntitySet(EdmEntitySet edmEntitySet, FilterOption filterOption,
			SelectOption selectOption, ExpandOption expandOption, OrderByOption orderByOption,
			SkipOption skipOption, TopOption topOption){
		
		EdmEntityType type = edmEntitySet.getEntityType();
	    FullQualifiedName fqName = type.getFullQualifiedName();
		
	    QueryBuilder queryBuilder = createQueryBuilder(
                filterOption);
	    
	   
	    SearchRequestBuilder requestBuilder = client
                .prepareSearch(elasticIndex)
        //      .setTypes(fqName.getName())
                .setQuery(queryBuilder);
	    configureSearchQuery(requestBuilder, selectOption, orderByOption, skipOption, topOption);
	    
	    //configureSearchQuery(requestBuilder, selectOption,
        //        orderByOption, skipOption, topOption);
	    
	    SearchResponse response = requestBuilder.execute().actionGet();
	    EntityCollection entityCollection = new EntityCollection();
	    SearchHits hits = response.getHits();
	    for (SearchHit searchHit : hits) {
	        Entity entity = convertHitToEntity(searchHit, type);
	        entity.setId(createId(searchHit.id()));
	        entity.setType(fqName.getFullQualifiedNameAsString());
	        entityCollection.getEntities().add(entity);
	    }
	    return entityCollection;
	}
	
	private void configureSearchQuery(SearchRequestBuilder requestBuilder, SelectOption selectOption, OrderByOption orderByOption, SkipOption skipOption, TopOption topOption){
		if (selectOption!=null) {
	        for (SelectItem selectItem : selectOption.getSelectItems()) {
	            requestBuilder.addField(selectItem.getResourcePath()
	                                      .getUriResourceParts().get(0).toString());
	        }
	    }

	    if (topOption!=null) {
	        requestBuilder.setSize(topOption.getValue());
	    }

	    if (skipOption!=null) {
	        requestBuilder.setFrom(skipOption.getValue());
	    }
	}
	
	private URI createId(String id){
		try{
			return new URI(id);
		}
		catch(URISyntaxException e){
			throw new ODataRuntimeException("Unable to create id for entity: "+ id);
		}
	}
	
	public QueryBuilder createQueryBuilder(FilterOption filterOption) {
	    /*
		if (filterOption != null) {
	        Expression expression = filterOption.getExpression();
	        return expression.accept(
	             new ElasticSearchExpressionVisitor());
	    } else {
	    */	
	        return QueryBuilders.matchAllQuery();
	    
	    //}
	}
	
	private Entity convertHitToEntity(SearchHit searchHit, EdmEntityType type){
		Entity e = new Entity();
		Map <String, Object> source = null;
		source = convertSearchHit(searchHit, type);
		List <String> props = type.getPropertyNames();
		for (int i = 0; i < props.size(); i++){
			if (source.get(props.get(i)) != null){
				e.addProperty(new Property(type.getProperty(props.get(i)).getType().getFullQualifiedName().toString(), props.get(i), ValueType.PRIMITIVE, source.get(props.get(i))));
			}
		}
		
		return e;
	}
	
	private Map <String, Object> convertSearchHit(SearchHit searchHit, EdmEntityType type){
		Map <String, Object> source = searchHit.sourceAsMap();
		Map <String, Object> converted = new HashMap <String, Object> ();
		
		for (Map.Entry<String, Object> entry: source.entrySet()){
			EdmElement currentElement = type.getProperty(entry.getKey());
			if (currentElement != null){
				
				if (currentElement.getType().getName().equals("DateTimeOffset")){
					String dateString = entry.getValue().toString();
					// 2014-08-29T11:21:53.092
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
					Date date = new Date(0);
					try{
						date = sdf.parse(dateString);
					}
					catch (ParseException e){
						e.printStackTrace();
					}
					converted.put(entry.getKey(), date.getTime());
				}
				else if (currentElement.getType().getName().equals("Double")){
					String doubleString = entry.getValue().toString();
					double val = -1;
					try{
						val = Double.parseDouble(doubleString);
					}
					catch(NumberFormatException e){
						e.printStackTrace();
					}
					converted.put(entry.getKey(), val);
				}
				else{
					converted.put(entry.getKey(), entry.getValue());
				}
				
			}
		}
		converted.put("id", searchHit.id());
		return converted;
	}



}
