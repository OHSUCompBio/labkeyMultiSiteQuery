package edu.ohsu.ccc;

import edu.ohsu.ccc.labkey.MultiSiteQuery;
import org.apache.commons.cli.*;

import java.io.File;
import java.util.List;

public class Main
{
    /**
     *
     * @param args: xmlConfig, outputTxt, username, password, filters
     * @throws Exception
     */
    public static void main(String[] args)
    {
        // create the command line parser
        CommandLineParser parser = new GnuParser();

        // create the Options
        Options options = new Options();
        options.addOption(OptionBuilder.withLongOpt("help").hasArg(false).withDescription("prints this message").create("h"));

        options.addOption(OptionBuilder.withArgName("xml_file").withLongOpt("xml").hasArg().withDescription("the path to the XML file with LabKey Server data source config").create("x"));
        options.addOption(OptionBuilder.withArgName("output_tsv").withLongOpt("output").hasArg().withDescription("the file where the output TSV will be written.  If null stdout will be used.").create("o"));
        options.addOption(OptionBuilder.withArgName("username").withLongOpt("username").hasArg().withDescription("the valid username for the LabKey Server(s)").create("u"));
        options.addOption(OptionBuilder.withArgName("password").withLongOpt("password").hasArg().withDescription("the valid password for the LabKey Server(s)").create("p"));
        options.addOption(OptionBuilder.withArgName("noHeaders").withLongOpt("noHeaders").hasArg(false).withDescription("if set, no headers will be written to the output").create("nh"));
        options.addOption(OptionBuilder.withArgName("includeSiteNameInOutput").withLongOpt("includeSiteNameInOutput").hasArg(false).withDescription("if set, the name of the source site will be appended as the first column").create("sn"));
        options.addOption(OptionBuilder.hasOptionalArgs().withArgName("filterExpression").withLongOpt("filter").withType(String.class).withDescription("a filter expression that will be passed to the server, in the form 'fieldName~eq=filterTerm'.  More than one filter can be accepted").create("f"));

        try
        {
            CommandLine line = parser.parse(options, args);

            // validate that block-size has been set
            if (line.hasOption("help"))
            {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp( "ant", options );
            }
            else
            {
                if (!line.hasOption("x") || !line.hasOption("u") || !line.hasOption("p"))
                {
                    throw new Exception("missing one or more required arguments.  please use -h to see the supported arguments.");
                }

                File xml = new File(line.getOptionValue("xml"));
                if (!xml.exists())
                {
                    throw new Exception("Unable to find XML config file: " + line.getOptionValue("xml"));
                }

                //this will validate our XML and throw exceptions
                MultiSiteQuery m = new MultiSiteQuery(xml);

                //TODO: should credentials be read from a file, like .netrc?
                String[] filters = line.getOptionValues("f");

                m.executeQuery(line.hasOption("o") ? new File(line.getOptionValue("output")) : null, line.getOptionValue("username"), line.getOptionValue("password"), line.hasOption("nh"), line.hasOption("sn"), filters);
            }
        }
        catch (Exception e)
        {
            System.err.println("Unexpected exception: " + e.getMessage());
            System.exit(1);
        }
    }
}
