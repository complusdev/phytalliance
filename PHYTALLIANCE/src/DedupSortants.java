import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DedupSortants {

	public static void main(String[] args) {
		// Objectif : Lire les appels sortants à appeler et vérifier si présence dans les appels entrants
		
		System.out.println("debut du programme");
		
		// Déclarations
		String url = "jdbc:sqlserver://192.168.193.236:1433;databaseName=ADL_PARTNER;";
		Connection cnn = null;
		Statement staSortant = null;
		Statement staEntrant = null;
		Statement staUpdate = null;
		
		ResultSet rsSortant = null;
		ResultSet rsEntrant = null;
		
		String req = "SELECT TEL1, TEL2, INDICE from C3_ADL_IB_CADU_1712_SPHY_SORTANT_1 WHERE PRIORITE IN (0,1)";
		
		try {Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");}
		catch (ClassNotFoundException e) {System.err.println("erreur de chargement du driver : " + e);}
		
		
		try {
			cnn = DriverManager.getConnection(url,"sa","sa");
			staSortant = cnn.createStatement();
			staEntrant = cnn.createStatement();
			staUpdate = cnn.createStatement();
			
			
			rsSortant = staSortant.executeQuery(req);
			while (rsSortant.next()) {
				String tel1 = rsSortant.getString("TEL1");
				String tel2 = rsSortant.getString("TEL2");
				int indice = rsSortant.getInt("indice");

				
				//Recherche de doublons dans la table des appels entrants
				int doublon = 0; // Doublon = 0 => pas en doublon 
				if (tel1!=null) {
					String reqEntrant = "SELECT COUNT(TEL) AS NBRE FROM [ADL_PARTNER].[dbo].[ADL_IB_CADU_1801_CPHY_ENTRANT] WHERE TEL = '"+tel1 +"'";	
					rsEntrant = staEntrant.executeQuery(reqEntrant);
				 	if ( rsEntrant.next()) {
				 		int nombreDoublon = rsEntrant.getInt("NBRE");
						if (nombreDoublon>0) {
							doublon = 1;
						}
				 	}	
				}
				
				
				if (tel2!=null) {
					String reqEntrant = "SELECT COUNT(TEL) AS NBRE FROM [ADL_PARTNER].[dbo].[ADL_IB_CADU_1801_CPHY_ENTRANT] WHERE TEL = '"+tel2 +"'";
					rsEntrant = staEntrant.executeQuery(reqEntrant);
				 	if ( rsEntrant.next()) {
				 		int nombreDoublon = rsEntrant.getInt("NBRE");
						if (nombreDoublon>0) {
							doublon = 1;
						}
				 	}	
				}
				
				
				if(doublon==1) {
					// Appel en doublon => update de PRIORITE = '-110'  : doublon entrant
					String reqUpdate = "UPDATE C3_ADL_IB_CADU_1712_SPHY_SORTANT_1 SET PRIORITE = '-110' WHERE INDICE = "+Integer.toString(indice);
					staUpdate.executeUpdate(reqUpdate);
					System.out.println("Doublon : "+ tel1 + " - "+ tel2);
					
					
				}
				
				
			}
			
			
			
			
			staUpdate.close();
			rsEntrant.close();
			staEntrant.close();
			rsSortant.close();
			staSortant.close();
			cnn.close();
			
			System.out.println("Fin du programme");
			
			
		} catch (SQLException e) {e.printStackTrace();}
		
		
		
		
		
		
		
		
		
		
		/**
		SELECT *
		  FROM [ADL_PARTNER].[dbo].[ADL_IB_CADU_1712_CPHY_ENTRANT]
		**/
	}

}
