package org.mark.kettle;

import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.RepositoryElementMetaInterface;
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;
import org.pentaho.di.repository.kdr.KettleDatabaseRepositoryMeta;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.addsequence.AddSequenceMeta;
import org.pentaho.di.trans.steps.dbproc.DBProcMeta;
import org.pentaho.di.trans.steps.dummytrans.DummyTransMeta;
import org.pentaho.di.trans.steps.rowgenerator.RowGeneratorMeta;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.job.JobHopMeta;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entries.abort.JobEntryAbort;
import org.pentaho.di.job.entries.special.JobEntrySpecial;
import org.pentaho.di.job.entries.success.JobEntrySuccess;
import org.pentaho.di.job.entries.writetolog.JobEntryWriteToLog;
import org.pentaho.di.job.entry.JobEntryCopy;

import java.text.SimpleDateFormat;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.NotePadMeta;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws Exception 
    {
        //System.out.println( "Hello World!" );
    	KettleEnvironment.init();
    	
    	DatabaseMeta databaseMeta = new DatabaseMeta("local_mysql","MYSQL","Native","localhost","kettle_repo","3306","root","1234");
    	
    	KettleDatabaseRepositoryMeta repositoryMeta = new KettleDatabaseRepositoryMeta( "kettle_repo", "kettle_repo", "", databaseMeta );
    	
    	KettleDatabaseRepository repository = new KettleDatabaseRepository();
        repository.init( repositoryMeta );
        
        repository.connect( "admin", "admin" );
        
        if (repository.isConnected()) {
        	System.out.println( "Connected" );
        }
        
        RepositoryDirectoryInterface tree = repository.loadRepositoryDirectoryTree();
        
        RepositoryDirectoryInterface dumpDirectory = repository.findDirectory("/dump");
        RepositoryDirectoryInterface sdxDirectory = tree.findDirectory( "/sandbox" );
        
        String[] spArr = {"SP_ONE","SP_TWO","SP_THREE","SP_FOUR"};
        
        for (RepositoryElementMetaInterface e : repository.getJobAndTransformationObjects( sdxDirectory.getObjectId(), false )) {
        	// == and != work on object identity. 
        	// While the two String s have the same value, they are actually two different objects. 
        	// you can use equals() method to statisfy your demands. == in java programming language has a different meaning!
    		if ( e.getName().equals("call_sp_example") ) {
    			TransMeta tranMeta = repository.loadTransformation(e.getObjectId(),null);
    			// copy template transformation meta
    			TransMeta copyMeta = (TransMeta)tranMeta.clone();
    			String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new java.util.Date());
    			copyMeta.setName("TRANS_BUILDER_"+timeStamp);
    			NotePadMeta note = new NotePadMeta("auto created by trans-builder", 100, 200, 100 , 20); 
    			copyMeta.addNote(note);
    			copyMeta.removeStep(0);
    			//System.out.println(tranMeta.getXML());
    			//System.out.println(copyMeta.getXML());

    			for ( StepMeta stepMeta : tranMeta.getTransHopSteps(true) ) {
    				// http://javadoc.pentaho.com/kettle/org/pentaho/di/trans/step/StepMetaInterface.html
    				 StepMetaInterface stepMetaInterface = stepMeta.getStepMetaInterface();
    				 //System.out.println(stepMetaInterface.getDialogClassName());
    				 //System.out.println(stepMetaInterface.getXML() );
    				 if ( stepMetaInterface instanceof DBProcMeta) {
    					 System.out.println(stepMetaInterface.getClass() );
    					 DBProcMeta dbProcMeta = (DBProcMeta)stepMetaInterface.clone();
    					 
    					 int idx = 1;
    					 for ( String spName : spArr ) {  						 
    						 dbProcMeta.setProcedure(spName);
    						 //System.out.println(dbProcMeta.getXML());
    						 
    						 StepMeta dbStepMeta = new StepMeta(spName, dbProcMeta);
    						 dbStepMeta.setDraw( true ); // display in editor
    						 dbStepMeta.setLocation( 100*idx, 100 );
    						 copyMeta.addStep(dbStepMeta);
    						 
    						 idx+=1;
    					 }
    					 copyMeta.setRepositoryDirectory( dumpDirectory );
    					 repository.save( copyMeta, "unit testing", null, true );
    					 break;
    				 }
    			}
    			
    		}
        }
    	
    	/* loop directory */
        //walkDirectory(repository,dumpDirectory);
    	
        /* add trans sample 
        TransMeta transMeta = generateTransformation();
        
        transMeta.setRepositoryDirectory( dumpDirectory );
        repository.save( transMeta, "unit testing", null, true );
        */
        
        /* add job sample
        JobMeta transMeta = generateJob();
        
        transMeta.setRepositoryDirectory( dumpDirectory );
        repository.save( transMeta, "unit testing", null, true );
        */
        
        

    }
    
    public static void walkDirectory(KettleDatabaseRepository repository,RepositoryDirectoryInterface dumpDir) throws Exception {
    	//dumpDir.getChildren().isEmpty())
    	for (RepositoryElementMetaInterface trans : repository.getTransformationObjects( dumpDir.getObjectId(), false )) {
    		String transDetail = String.format("ObjectType: %1$s ObjectId: %2$s ObjectName: %3$s ;", trans.getObjectType(), trans.getObjectId(), trans.getName());
    		System.out.println(transDetail);
    	}
    	
    	for (RepositoryElementMetaInterface job : repository.getJobObjects( dumpDir.getObjectId(), false )) {
    		String jobDetail = String.format("ObjectType: %1$s ObjectId: %2$s ObjectName: %3$s ;", job.getObjectType(), job.getObjectId(), job.getName());
    		System.out.println(jobDetail);
    	}
    	
    	for ( RepositoryDirectoryInterface subDir : dumpDir.getChildren() ) {
    		walkDirectory(repository,subDir);
    	}
    }
    
    public static TransMeta generateTransformation() {
        try {
          System.out.println( "Generating a transformation definition" );

          // create empty transformation definition
          TransMeta transMeta = new TransMeta();
          transMeta.setName( "Generated Demo Transformation" );

          // The plug-in registry is used to determine the plug-in ID of each step used 
          PluginRegistry registry = PluginRegistry.getInstance();

          // ------------------------------------------------------------------------------------ 
          // Create Row Generator Step and put it into the transformation
          // ------------------------------------------------------------------------------------
          System.out.println( "- Adding Row Generator Step" );

          // Create Step Definition and determine step ID 
          RowGeneratorMeta rowGeneratorMeta = new RowGeneratorMeta();
          String rowGeneratorPluginId = registry.getPluginId( StepPluginType.class, rowGeneratorMeta );

          // Step it is configured to generate 5 rows with 2 fields
          // field_1: "Hello World" (PDI Type: String)
          // field_2: 100 (PDI Type: Integer)

          rowGeneratorMeta.setRowLimit( "5" );

          rowGeneratorMeta.allocate( 2 );
          rowGeneratorMeta.setFieldName( new String[] { "field_1", "field_2" } );
          rowGeneratorMeta.setFieldType( new String[] { "String", "Integer" } );
          rowGeneratorMeta.setValue( new String[] { "Hello World", "100" } );

          StepMeta rowGeneratorStepMeta = new StepMeta( rowGeneratorPluginId, "Generate Some Rows", rowGeneratorMeta );

          // make sure the step appears on the canvas and is properly placed in spoon
          rowGeneratorStepMeta.setDraw( true );
          rowGeneratorStepMeta.setLocation( 100, 100 );

          // include step in transformation
          transMeta.addStep( rowGeneratorStepMeta );

          // ------------------------------------------------------------------------------------ 
          // Create "Add Sequence" Step and connect it the Row Generator
          // ------------------------------------------------------------------------------------
          System.out.println( "- Adding Add Sequence Step" );

          // Create Step Definition 
          AddSequenceMeta addSequenceMeta = new AddSequenceMeta();
          String addSequencePluginId = registry.getPluginId( StepPluginType.class, addSequenceMeta );

          // configure counter options
          addSequenceMeta.setDefault();
          addSequenceMeta.setValuename( "counter" );
          addSequenceMeta.setCounterName( "counter_1" );
          addSequenceMeta.setStartAt( 1 );
          addSequenceMeta.setMaxValue( Long.MAX_VALUE );
          addSequenceMeta.setIncrementBy( 1 );

          StepMeta addSequenceStepMeta = new StepMeta( addSequencePluginId, "Add Counter Field", addSequenceMeta );

          // make sure the step appears on the canvas and is properly placed in spoon
          addSequenceStepMeta.setDraw( true );
          addSequenceStepMeta.setLocation( 300, 100 );

          // include step in transformation
          transMeta.addStep( addSequenceStepMeta );

          // connect row generator to add sequence step
          transMeta.addTransHop( new TransHopMeta( rowGeneratorStepMeta, addSequenceStepMeta ) );

          // ------------------------------------------------------------------------------------ 
          // Add a "Dummy" Step and connect it the previous step
          // ------------------------------------------------------------------------------------
          System.out.println( "- Adding Dummy Step" );
          // Create Step Definition 
          DummyTransMeta dummyMeta = new DummyTransMeta();
          String dummyPluginId = registry.getPluginId( StepPluginType.class, dummyMeta );

          StepMeta dummyStepMeta = new StepMeta( dummyPluginId, "Dummy", dummyMeta );

          // make sure the step appears alright in spoon
          dummyStepMeta.setDraw( true );
          dummyStepMeta.setLocation( 500, 100 );

          // include step in transformation
          transMeta.addStep( dummyStepMeta );

          // connect row generator to add sequence step
          transMeta.addTransHop( new TransHopMeta( addSequenceStepMeta, dummyStepMeta ) );

          return transMeta;
        } catch ( Exception e ) {
          // something went wrong, just log and return
        	System.out.println(e.getMessage());
        	return null;
        }
      }
    
    public static JobMeta generateJob() {

        try {
          System.out.println( "Generating a job definition" );

          // create empty transformation definition
          JobMeta jobMeta = new JobMeta();
          jobMeta.setName( "Generated Demo Job" );

          // ------------------------------------------------------------------------------------ 
          // Create start entry and put it into the job
          // ------------------------------------------------------------------------------------
          System.out.println( "- Adding Start Entry" );

          // Create and configure start entry
          JobEntrySpecial start = new JobEntrySpecial();
          start.setName( "START" );
          start.setStart( true );

          // wrap into JobEntryCopy object, which holds generic job entry information
          JobEntryCopy startEntry = new JobEntryCopy( start );

          // place it on Spoon canvas properly
          startEntry.setDrawn( true );
          startEntry.setLocation( 100, 100 );

          jobMeta.addJobEntry( startEntry );

          // ------------------------------------------------------------------------------------ 
          // Create "write to log" entry and put it into the job
          // ------------------------------------------------------------------------------------
          System.out.println( "- Adding Write To Log Entry" );

          // Create and configure entry
          JobEntryWriteToLog writeToLog = new JobEntryWriteToLog();
          writeToLog.setName( "Output PDI Stats" );
          writeToLog.setLogLevel( LogLevel.MINIMAL );
          writeToLog.setLogSubject( "Logging PDI Build Information:" );
          writeToLog.setLogMessage( "Version: ${Internal.Kettle.Version}\n"
            + "Build Date: ${Internal.Kettle.Build.Date}" );

          // wrap into JobEntryCopy object, which holds generic job entry information
          JobEntryCopy writeToLogEntry = new JobEntryCopy( writeToLog );

          // place it on Spoon canvas properly
          writeToLogEntry.setDrawn( true );
          writeToLogEntry.setLocation( 200, 100 );

          jobMeta.addJobEntry( writeToLogEntry );

          // connect start entry to logging entry using simple hop
          jobMeta.addJobHop( new JobHopMeta( startEntry, writeToLogEntry ) );

          // ------------------------------------------------------------------------------------ 
          // Create "success" entry and put it into the job
          // ------------------------------------------------------------------------------------
          System.out.println( "- Adding Success Entry" );

          // crate and configure entry
          JobEntrySuccess success = new JobEntrySuccess();
          success.setName( "Success" );

          // wrap into JobEntryCopy object, which holds generic job entry information
          JobEntryCopy successEntry = new JobEntryCopy( success );

          // place it on Spoon canvas properly
          successEntry.setDrawn( true );
          successEntry.setLocation( 400, 100 );

          jobMeta.addJobEntry( successEntry );

          // connect logging entry to success entry on TRUE evaluation 
          JobHopMeta greenHop = new JobHopMeta( writeToLogEntry, successEntry );
          greenHop.setEvaluation( true );
          jobMeta.addJobHop( greenHop );

          // ------------------------------------------------------------------------------------ 
          // Create "abort" entry and put it into the job
          // ------------------------------------------------------------------------------------
          System.out.println( "- Adding Abort Entry" );

          // crate and configure entry
          JobEntryAbort abort = new JobEntryAbort();
          abort.setName( "Abort Job" );

          // wrap into JobEntryCopy object, which holds generic job entry information
          JobEntryCopy abortEntry = new JobEntryCopy( abort );

          // place it on Spoon canvas properly
          abortEntry.setDrawn( true );
          abortEntry.setLocation( 400, 300 );

          jobMeta.addJobEntry( abortEntry );

          // connect logging entry to abort entry on FALSE evaluation 
          JobHopMeta redHop = new JobHopMeta( writeToLogEntry, abortEntry );
          redHop.setEvaluation( false );
          jobMeta.addJobHop( redHop );

          return jobMeta;

        } catch ( Exception e ) {

          // something went wrong, just log and return
          //e.printStackTrace();
        System.out.println(e.getMessage());
          return null;
        }
      }

}
