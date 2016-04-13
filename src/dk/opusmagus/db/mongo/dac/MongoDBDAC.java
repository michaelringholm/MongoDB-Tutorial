package dk.opusmagus.db.mongo.dac;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Pattern;

import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.CommandResult;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.QueryBuilder;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;

import static java.util.Arrays.asList;

public class MongoDBDAC {

	private MongoClient mongoClient;
	private MongoDatabase db; 
	
	public MongoDBDAC()
	{
		mongoClient = new MongoClient();
		db = mongoClient.getDatabase("test");
	}
	
	public static void main(String[] args) {
		System.out.println("MongoDB tutorial started...");
		Date start = new Date();
		
		MongoDBDAC dac = new MongoDBDAC();
		
		try {
			//dac.createMassData();
			//dac.getDocuments("trades");
			//dac.getDocumentsByTopLevelFilter("trades");
			dac.getDocumentsByEmbeddedFilter("trades");
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
		
		Date end = new Date();
		System.out.println("MongoDB tutorial ended in " + String.valueOf(end.getTime()-start.getTime()) + " ms!");
	}
	
	private void getDocuments(String collectionName)
	{
		FindIterable<Document> iterable = db.getCollection(collectionName).find();
		
		iterable.forEach(new Block<Document>() {
		    @Override
		    public void apply(final Document document) {
		        System.out.println(document);
		    }
		});
	}
	
	private void getDocumentsByTopLevelFilter(String collectionName)
	{
		//MongoCollection<Document> iterable = db.getCollection(collectionName).find(eq("borough", "Manhattan"));
		FindIterable<Document> iterable = db.getCollection(collectionName).find(
		        new Document("_id", "565432cb8d1c773e4430ade3"));
		
		iterable.forEach(new Block<Document>() {
		    @Override
		    public void apply(final Document document) {
		        System.out.println(document);
		    }
		});
	}
	
	private void getDocumentsByEmbeddedFilter(String collectionName)
	{
		//MongoCollection<Document> iterable = db.getCollection(collectionName).find(eq("borough", "Manhattan"));
		FindIterable<Document> iterable = db.getCollection(collectionName).find(
		        new Document("trade.tradeType", "Floor"));
		//FindIterable<Document> iterable = db.getCollection(collectionName).find(
		        //new Document("tradeType", "Swap"));
		
		iterable.forEach(new Block<Document>() {
		    @Override
		    public void apply(final Document document) {
		        System.out.println(document);
		    }
		});
	}
	
	private void getDocumentsByFuzzyFilter(String collectionName)
	{
		//DBObject q = QueryBuilder.start("trade.tradeType").is(Pattern.compile("oo", 
          //      Pattern.CASE_INSENSITIVE)).get();
		MongoCollection<Document> coll = db.getCollection(collectionName);
		//coll.find(new Document("trade.tradeType", "Floor"));
		
		//MongoCollection<Document> iterable = db.getCollection(collectionName).find(eq("borough", "Manhattan"));
		//FindIterable<Document> iterable = db.getCollection(collectionName).find(
		  //      new Document("trade.tradeType", "Floor"));
		//FindIterable<Document> iterable = db.getCollection(collectionName).find(
		        //new Document("tradeType", "Swap"));
		
	    /*DBObject textSearchCommand = new BasicDBObject();
	    textSearchCommand.put("text", collectionName);
	    textSearchCommand.put("search", "oo");
	    CommandResult commandResult = db.runCommand(textSearchCommand);
		
		coll.forEach(new Block<Document>() {
		    @Override
		    public void apply(final Document document) {
		        System.out.println(document);
		    }
		});*/
	}

	private void createMassData() throws Exception {
		for(int i=0; i<5; i++)
			insertDocument("trades");
	}
	
	private void dropCollection(String collectionName)
	{
		db.getCollection(collectionName).drop();
	}
	
	private void createIndex(String collectionName)
	{
		db.getCollection(collectionName).createIndex(new Document("trade.tradeType", 1));
	}

	private void insertDocument(String collectionName) throws Exception {
		/* If the document passed to the insertOne method does not contain the _id field, 
		 * the driver automatically adds the field to the document and sets the field’s value 
		 * to a generated ObjectId.	 */
		
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
		db.getCollection(collectionName).insertOne(
	        new Document("trade",
                new Document()
	        	.append("accountNo", "11232-333-23123-33")
                .append("tradeType", "Floor")
                .append("tradeAmount", new Date().getTime())
                .append("tradeDate", format.format(new Date()))
                ));
                /*.append("tradeMetaData", asList(
                        new Document()
                                .append("date", format.parse("2014-10-01T00:00:00Z"))
                                .append("grade", "A")
                                .append("score", 11),
                        new Document()
                                .append("date", format.parse("2014-01-16T00:00:00Z"))
                                .append("grade", "B")
                                .append("score", 17)))
                .append("name", "Vella")
                .append("restaurant_id", "41704620"));*/
	}

}
