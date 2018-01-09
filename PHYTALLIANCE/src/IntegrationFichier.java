import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class IntegrationFichier {

	public static void main(String[] args) {
		
		// PARAMETRE A MODIFIER
		String fichierIn = "C:/Users/hbleu/eclipse-workspace/PHYTALLIANCE/files/20171228_PHYTALLIANCE_sortie.txt";
		String fichierOut = "C:/Users/hbleu/eclipse-workspace/PHYTALLIANCE/files/20171228_PHYTALLIANCE_out.csv";
		String fichierError = "C:/Users/hbleu/eclipse-workspace/PHYTALLIANCE/files/20171228_PHYTALLIANCE_error.csv";
		
		
		
		
		System.out.println("debut du programme");
		
		// Déclarations
		String url = "jdbc:sqlserver://192.168.193.236:1433;databaseName=ADL_PARTNER;";
		Connection cnn = null;
		Statement sta = null; 
		ResultSet rs = null;
		InputStream ips = null;
		String ligne  = null;
		
		
		try {Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");}
		catch (ClassNotFoundException e) {System.err.println("erreur de chargement du driver : " + e);}
		
		try {
			ips = new FileInputStream(fichierIn);
			InputStreamReader ipsr=new InputStreamReader(ips);
			BufferedReader br=new BufferedReader(ipsr);
			
			PrintWriter pw = new PrintWriter(new FileWriter(fichierOut));
			PrintWriter pwError = new PrintWriter(new FileWriter(fichierError));
			pw.println("CODE_FICH_PART;NUM_CLIENT;CODE_MAILING;NOMPRENOM;ADRESSE;ADRESSE2;CP;VILLE;CIVILITE;PRENOM;NOM;TEL_FIXE;DATE_NAISSANCE;NUM_CLIENT_EXTERNE;TEL_PORT");
			
			cnn = DriverManager.getConnection(url,"sa","sa");
			sta = cnn.createStatement();
		    
			String ligneOut = "";
			int compteur = 0;
			int compteurErreur = 0;
			while ((ligne=br.readLine())!=null) {
				
				//Schéma du fichier client fourni
				String societe = ligne.substring(0, 3);
				String date_crea = ligne.substring(3, 11);
				String origine = ligne.substring(11, 14);
				String num_rapprochement = ligne.substring(14, 21);
				String num_prospect = ligne.substring(21, 30);
				String num_regpt = ligne.substring(30, 34);
				String cde_mailing = ligne.substring(34, 40);
				String cde_insee = ligne.substring(40, 50);
				String volet1 = ligne.substring(50, 88);
				String volet2 = ligne.substring(88, 126);
				String volet3 = ligne.substring(126, 164);
				String volet4 = ligne.substring(164, 202);
				String volet5 = ligne.substring(202, 240);
				String volet6 = ligne.substring(240, 278);
				
				String civilite = ligne.substring(278, 290);
				String prenom = ligne.substring(290, 320);
				String nom = ligne.substring(320, 350);
				String telephone_mobile = ligne.substring(350, 360);
				String naissance = ligne.substring(360, 368);
				String email = ligne.substring(368, 418);
				String telephone_fixe = ligne.substring(580, 590);
				String date_naissance  = ligne.substring(360, 368);
				String identifiant_client_externe = ligne.substring(487, 507);
				
				// Variable pour la construction du fichier à intégrer
				String code_fiche_part = societe+date_crea;
				String nomprenom = volet1;
				String adresse ="";
				String adresse2 = "";
				String cp ="";
				String ville ="";
						
				
				
				// Recherche des doublons dans la base des appels sortants
			 	String req = "SELECT COUNT(*) AS nbre FROM [ADL_PARTNER].[dbo].C3_ADL_IB_CADU_1712_SPHY_SORTANT_1 WHERE "
			 			+ "(TEL1 IN ('"+telephone_mobile +"','"+telephone_fixe + "') OR TEL2 IN ('"+telephone_mobile +"','"+telephone_fixe + "')) "
			 			+ "AND (PRIORITE = 0 OR PRIORITE = -1 OR PRIORITE =-10)" ;

			 	rs = sta.executeQuery(req);
			 	if ( rs.next()) {
			 		int nombreDoublon = rs.getInt("nbre");
					if (nombreDoublon>0) {
						compteurErreur = compteurErreur+1;						
					}
					else {
						ligneOut = code_fiche_part+ ";"+num_prospect+";"+cde_mailing+";"+nomprenom+";"+ adresse+";"+adresse2+";"+cp+";"+
								   ville+";"+civilite+";"+prenom+";"+nom+";"+telephone_fixe+";"+date_naissance+";"+identifiant_client_externe+";"+telephone_mobile;
						pw.println(ligneOut);
					}//fin If doublon	
				}//fin if rs.next
				
				compteur = compteur+1;
				
				// Recherche des doublons dans la appels entrants
				// NE PAS FAIRE INUTILE
				
				
			}

			System.out.println("RAPPORT DU TRAITEMENT");
			System.out.println("----------------------");
			System.out.println("NOMBRE DE LIGNES LUES : "+Integer.toString(compteur));
			System.out.println("NOMBRE DE LIGNES TRAITEES : "+Integer.toString(compteur - compteurErreur));
			
			
			
			pw.close();
			pwError.close();
			rs.close();
			sta.close();
			cnn.close();
			
		} catch (IOException | SQLException e) {
			e.printStackTrace();
		}
		
		
		
		
	}

}
