/**
 * 
 */
package com.eventshop.eventshoplinux.util.datasourceUtil.wrapper;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.SearchResult.Hit;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.LinkedBlockingQueue;

import com.eventshop.eventshoplinux.domain.common.FrameParameters;
import com.eventshop.eventshoplinux.domain.datasource.emage.STTPoint;
import com.eventshop.eventshoplinux.util.commonUtil.Config;
import com.eventshop.eventshoplinux.util.commonUtil.GeoLocation;
import com.eventshop.eventshoplinux.util.commonUtil.Query;

import static org.elasticsearch.index.query.FilterBuilders.rangeFilter;
import static org.elasticsearch.index.query.QueryBuilders.filteredQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import twitter4j.Status;

/**
 * @author Ashok
 * 
 */
public class ElasticSearchWrapper extends TwitterWrapper {

	private JestClient client = null;
	private SearchSourceBuilder searchSourceBuilder = null;
	private Search search;
	private Query query;

	public ElasticSearchWrapper(String url, String theme,
			FrameParameters params, boolean saveTweets) {
		super(url, theme, params, saveTweets);

		ls = new LinkedBlockingQueue<STTPoint>();

		this.saveTweets = saveTweets;
		if (this.saveTweets) {
			this.tableName = "tbl_" + theme + "_es";
			connection();
			createTweetTable(theme);
		}
	}

	public Connection connection() {
		Connection conn = super.connection();
		client = openClient();
		query = new Query();

		search = (Search) new Search.Builder(query.getQuery()).addIndex(theme)
				.addType(theme + "_type").build();

		return conn;
	}

	private JestClient openClient() {
		HttpClientConfig clientConfig = new HttpClientConfig.Builder(
				"http://localhost:9200").multiThreaded(true).build();
		JestClientFactory factory = new JestClientFactory();

		factory.setHttpClientConfig(clientConfig);
		JestClient jestClient = factory.getObject();

		return jestClient;
	}

	public void run() {
		isRunning = true;

		try {
			getPopulation();
		} catch (IOException e1) {
			log.error(e1.getMessage());
		}

		int numofRows = params.getNumOfRows();
		int numOfColumns = params.getNumOfColumns();
		// Initialize the SinceID matrix
		sinceID = new long[numofRows][numOfColumns];
		for (int i = 0; i < numofRows; ++i)
			for (int j = 0; j < numOfColumns; ++j)
				sinceID[i][j] = 0;

		// Setting the bagOfWords for search
		String queryStr = bagOfWords[0];
		for (int i = 1; i < bagOfWords.length; i++)
			queryStr += (", " + bagOfWords[i]);

		// Set end time
		long endTime = (long) Math.ceil(System.currentTimeMillis()
				/ params.timeWindow)
				* params.timeWindow + params.syncAtMilSec;

		while (isRunning) {
			STTPoint point;

			Date start = new Date(endTime - params.timeWindow);

			int numQueries = 0;
			// Loop for Latitude Longitude blocks
			for (double i = params.swLat; i < params.neLat; i = i
					+ params.latUnit) {
				for (double j = params.swLong; j < params.neLong; j = j
						+ params.longUnit) {
					if (!isRunning)
						break;

					int y = (int) Math.ceil(Math.abs((BigDecimal.valueOf(j))
							.subtract(BigDecimal.valueOf(params.swLong))
							.divide(BigDecimal.valueOf(params.longUnit))
							.doubleValue()));
					int x = (int) Math.ceil(Math.abs((BigDecimal.valueOf(i))
							.subtract(BigDecimal.valueOf(params.swLat))
							.divide(BigDecimal.valueOf(params.latUnit))
							.doubleValue()));

					int ret = 0;
					if (isPopulated[12 - x][y]) {
						ret = doCollection(i + 0.5 * params.latUnit, j + 0.5
								* params.longUnit, x, y, params.latUnit, start,
								queryStr);
						numQueries++;
					}
					point = new STTPoint(ret, start, new Date(endTime),
							params.latUnit, params.longUnit, i, j, theme);
					ls.add(point);
				}
			}
			log.info("ElasticSearchWrapper: NumQueries made:" + numQueries);

			// Sleeping when window is not up yet
			endTime += params.timeWindow;
			while (System.currentTimeMillis() < endTime) {
				try {
					Thread.sleep(endTime - System.currentTimeMillis());
				} catch (InterruptedException e) {
					log.error(e.getMessage());
				}
			}
		}
	}

	public int doCollection(double lat, double lng, int x, int y,
			double latUnit, Date date, String queryStr) {

		query.setCount(100);
		query.setGeoCode(new GeoLocation(lat, lng), (int) (60.0 * latUnit),
				Query.KILOMETERS);
		query.setOrMatchFilter("message", queryStr);
		query.setSinceId(sinceID[x][y]);
		query.setSince(new SimpleDateFormat("yyyy-MM-dd").format(date));

		boolean firstOne = true;
		int count = 0;
		
        try {
            SearchResult result = client.execute(search);

            List<Hit<SearchResultTuple, Void>> tweets = result.getHits(SearchResultTuple.class);
            for(Hit<SearchResultTuple, Void> atweet : tweets) {
            	SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
                SearchResultTuple tweet = atweet.source;
                ArrayList<Double>location = (ArrayList<Double>)tweet.getGeoLocation();
				result = client.execute(search);

					// Update sinceID of this block
					if (firstOne) {
						sinceID[x][y] = tweet.getId();
						firstOne = false;
					}
					if (this.saveTweets) {
						ArrayList<Double>geods = tweet.getGeoLocation();
						GeoLocation tw_loc = new GeoLocation(geods);
						double p_lat = 0;
						double p_lon = 0;
						if (tw_loc != null) {
							p_lat = tw_loc.getLatitude();
							p_lon = tw_loc.getLongitude();
						}
						insertTweet(lat, lng, x, y, tweet, p_lat, p_lon);
					}
					count++;
				}

		} catch (Exception e) {
			log.error("theme:" + theme + ", error: " + e.getMessage());
			try {
				if (e.getMessage().contains("500"))
					return 0;
				if (e.getMessage().contains("420")
						|| e.getMessage().contains("429")
						|| e.getMessage().contains("limit")) {
					Thread.sleep(1000 * 60 * 15);
					log.info("thread sleep for 15 minutes");
				}
			} catch (InterruptedException e2) {
				System.out.println("why error here?" + e2);
				log.error(e2.getMessage());
			}
			return -1;
		}
		return count;
	}

	public boolean stop() {
		isRunning = false;
		Thread.currentThread().interrupt();
		return true;
	}

	public STTPoint next() {
		try {
			return ls.take();
		} catch (InterruptedException e) {
			log.error(e.getMessage());
		}
		return null;
	}

	public boolean hasNext() {
		return (ls.peek() != null);
	}

	public void remove() {
		ls.remove();
	}

	public void getPopulation() throws IOException {
		BufferedReader br1 = new BufferedReader(new FileReader(
				Config.getProperty("tempDir") + "visual/newPopulation.txt"));
		String myline = "";
		// double startLat = 24.5;

		int numPop = 0;
		System.out.println("paramssw==" + params.swLat + "params.neLat "
				+ params.neLat + " now params.latUnit " + params.latUnit);
		System.out.println("J LOOP -===params.swLong==" + params.swLong
				+ "params.neLong " + params.neLong + " now params.longUnit "
				+ params.longUnit);
		for (double i = params.swLat; i < params.neLat; i += params.latUnit) {
			myline = br1.readLine();
			if (myline == null)
				break;
			StringTokenizer vals = new StringTokenizer(myline, " ,");
			log.info("size of vals:" + vals.countTokens());

			for (double j = params.swLong; j < params.neLong; j += params.longUnit) {
				if (vals.hasMoreTokens()) {
					String val = vals.nextToken();
					if (val.compareTo("1") == 0) {
						isPopulated[(int) ((i - params.swLat) / params.latUnit)][(int) ((j - params.swLong) / params.longUnit)] = true;
						numPop++;
					} else
						isPopulated[(int) ((i - params.swLat) / params.latUnit)][(int) ((j - params.swLong) / params.longUnit)] = false;
				}
			}
		}

		log.info("numPop:" + numPop);
	}

	private int insertTweet(double lat, double lng, int x, int y, SearchResultTuple tweet, double p_lat, double p_lon)
	{
		String text = textFilter(tweet.getText());
		if(text == null) return -1;

		String date = tweet.getCreatedAt();
		String sql = "INSERT INTO " + tableName + " VALUES (NULL, " +
			tweet.getId() + ", " + lat + ", " + lng + ", '" + 
			date + "', '" + text + "',"+p_lat+","+p_lon+", '"+textFilter(tweet.getUser())+"')";
		try {
			Statement statement = conn.createStatement();
			statement.execute(sql);
		} catch (SQLException e) {
			log.error(sql);
			log.error(e.getMessage());
			connection();
		}
		return 0;
	}

	class SearchResultTuple {
		private Object location;
		private String message;
		private String author;
		private String date;
		private long id;
		private String place;
		private String place_type;
		private String sentiment;
		private double polarity;
		private double subjectivity;

		public ArrayList<Double> getGeoLocation() {
			return (ArrayList<Double>) location;
		}
		public String getUser() {
			// TODO Auto-generated method stub
			return author;
		}
		public String getCreatedAt() {
			// TODO Auto-generated method stub
			return date;
		}
		public long getId() {
			return id;
		}
		public String getText() {
			return message;
		}
		public String getSentiment() {
			return sentiment;
		}
		public String getPlaceType() {
			return place_type;
		}
		public double getPolarity() {
			return polarity;
		}
		public double getSubjectivity() {
			return subjectivity;
		}
		public String getPlace() {
			return place;
		}
	};

}
