package com.eventshop.eventshoplinux.util.commonUtil;

import org.json.simple.JSONObject;

public class Query {
	public static final String KILOMETERS = "km";
	public static final String MILES = "m";

	private int count;
	private java.lang.String geocode;
	private String unit;
	private java.lang.String lang;
	private java.lang.String locale;

	private java.lang.String nextPageQuery;
	private java.lang.String query;
	private JSONObject oQuery;
	private static long serialVersionUID;
	private java.lang.String since;
	private long sinceId;
	private long maxId;
	private java.lang.String until;

	public Query() {
		oQuery = new JSONObject();
		count = 0;
		query = new String();
		unit = KILOMETERS;
	}

	public void count(int c) {
		count = c;
	}

	/**
	 * Returns the number of tweets to return per page, up to a max of 100
	 * 
	 * @return
	 */
	public int getCount() {
		return count;
	}

	/**
	 * Returns the specified query
	 * 
	 * @return
	 */
	public java.lang.String getQuery() {
		return oQuery.toString();
	}

	/**
	 * returns tweets by users located within a given radius of the given
	 * latitude/longitude, where the user's location is taken from their Twitter
	 * profile
	 * 
	 * @param location
	 * @param radius
	 * @param unit
	 */
	// {"geo_distance_range": { "distance" : "400km", "location": { "lat" :
	// 12.55, "lon" : -100.10 } }
	public void setGeoCode(GeoLocation location, int radius, String unit) {
		JSONObject json = new JSONObject();
		JSONObject jLoc = new JSONObject();
		JSONObject dLoc = new JSONObject();
		JSONObject filter = new JSONObject();

		jLoc.put("lat", location.getLatitude());
		jLoc.put("lon", location.getLongitude());
		dLoc.put("distance", String.valueOf(radius) + unit);
		dLoc.put("location", jLoc);
		filter.put("geo_distance_range", dLoc);
		oQuery.put("filter", filter);
	}

	/**
	 * returns tweets based on keywords
	 * 
	 * @param location
	 * @param radius
	 * @param unit
	 */
	public // { "query": { "match" : { "message" : { "query" : "life san", "operator" :
	// "or", "zero_terms_query": "all" } } } }
	void setOrMatchFilter(String field, String keywords) {
		JSONObject term = new JSONObject();
		term.put("query", keywords);
		term.put("operator", "or");
		term.put("zero_terms_query", "all");
		JSONObject message = new JSONObject();
		message.put(field, term);
		JSONObject match = new JSONObject();
		match.put("match", message);
		oQuery.put("query", match);
	}

	public void setCount(int i) {
		count = i;
	}

	public void setQuery(String queryStr) {
		this.query = queryStr;
	}

	public void setSinceId(long l) {
		sinceId = l;
	}

	public void setSince(String date) {
		since = date;
	}

};
