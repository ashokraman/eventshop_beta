package com.eventshop.eventshoplinux.util.commonUtil;

import java.util.ArrayList;

public class GeoLocation {

	ArrayList<Double>locations = null; 
	public GeoLocation (ArrayList<Double>locs) {
		locations = locs; 
	 }
	public GeoLocation (double lat, double lon) {
		if (locations == null)
			locations = org.elasticsearch.common.collect.Lists .newArrayList(0.0,0.0); 
		locations.add(0,lat);
		locations.add(1,lon);
	 }
	public double getLatitude() {
		 if (locations.size() >= 2) {
			 return locations.get(0);
		 }
		else
			return 0.0;
	 }
	public double getLongitude() {
		 if (locations.size() >= 2) {
			 return locations.get(1);
		 }
		else
			return 0.0;
	 }
}
