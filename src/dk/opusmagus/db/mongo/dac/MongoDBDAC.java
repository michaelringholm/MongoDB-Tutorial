package dk.opusmagus.db.mongo.dac;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

import com.mongodb.Block;
import com.mongodb.MongoClient;
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
		
		MongoDBDAC dac = new MongoDBDAC();
		
		try {
			dac.createMassData();
			dac.getDocuments("trades");
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println("MongoDB tutorial ended!");
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
		        new Document("tradeType", "FX Option"));
		
		iterable.forEach(new Block<Document>() {
		    @Override
		    public void apply(final Document document) {
		        System.out.println(document);
		    }
		});
	}

	private void createMassData() throws Exception {
		for(int i=0; i<500; i++)
			insertDocument("trades");
	}

	private void insertDocument(String collectionName) throws Exception {
		/* If the document passed to the insertOne method does not contain the _id field, 
		 * the driver automatically adds the field to the document and sets the field’s value 
		 * to a generated ObjectId.	 */
		
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH);
		db.getCollection(collectionName).insertOne(
	        new Document("trade",
                new Document()
	        	.append("accountNo", "11232-333-23123-33")
                .append("tradeType", "FX Option")
                .append("tradeAmount", "5200000")
                .append("tradeDate", format.parse("2014-10-01T00:00:00Z"))
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
