import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StudentOpportunityAnalysis {

    // Function for column 1: Hours studied
    public static double calculateHoursStudied(String input) {
        double num = Double.parseDouble(input);
        return (num / 44) * 100;
    }

    // Function for column 2: Attendance percentage
    public static double calculateAttendance(String input) {
        double num = Double.parseDouble(input);
        return num;  // Return the percentage as-is
    }

    // Function for column 3: Parental involvement
    public static double calculateParentalInvolvement(String input) {
        int num = convertLowMediumHigh(input);
        return (num / 3.0) * 100;
    }

    // Function for column 4: Access to resources
    public static double calculateAccessToResources(String input) {
        int num = convertLowMediumHigh(input);
        return (num / 3.0) * 100;
    }

    // Function for column 5: Extracurricular
    public static double calculateExtracurricular(String input) {
        return 0;
    }

    // Function for column 6: Sleep
    public static double calculateSleep(String input) {
        int num = Integer.parseInt(input);
        if (num == 9) return 90;
        if (num == 10) return 80;
        return 100;
    }

    // Function for column 7: Previous score
    public static double calculatePreviousScore(String input) {
        return 0;
    }

    // Function for column 8: Motivation level
    public static double calculateMotivationLevel(String input) {
        int num = convertLowMediumHigh(input);
        return (num / 3.0) * 100;
    }

    // Function for column 9: Internet access
    public static double calculateInternetAccess(String input) {
        int num = input.equalsIgnoreCase("No") ? 1 : 2;
        return (num / 2.0) * 100;
    }

    // Function for column 10: Tutoring session
    public static double calculateTutoringSession(String input) {
        double num = Double.parseDouble(input);
        return (num / 8.0) * 100;
    }

    // Function for column 11: Family income
    public static double calculateFamilyIncome(String input) {
        int num = convertLowMediumHigh(input);
        return (num / 3.0) * 100;
    }

    // Function for column 12: Teacher quality
    public static double calculateTeacherQuality(String input) {
        int num = convertLowMediumHigh(input);
        return (num / 3.0) * 100;
    }

    // Function for column 13: School type
    public static double calculateSchoolType(String input) {
        int num = input.equalsIgnoreCase("Public") ? 1 : 2;
        return (num / 2.0) * 100;
    }

    // Function for column 14: Peer influence
    public static double calculatePeerInfluence(String input) {
        int num = convertPeerInfluence(input);
        return (num / 3.0) * 100;
    }

    // Function for column 15: Physical activity
    public static double calculatePhysicalActivity(String input) {
        return 0;
    }

    // Function for column 16: Learning disabilities
    public static double calculateLearningDisabilities(String input) {
        int num = input.equalsIgnoreCase("Yes") ? 2 : 1;
        return (num / 2.0) * 100;
    }

    // Function for column 17: Parent education
    public static double calculateParentEducation(String input) {
        int num = convertParentEducation(input);
        return (num / 3.0) * 100;
    }

    // Function for column 18: Distance from school
    public static double calculateDistanceFromSchool(String input) {
        int num = convertDistance(input);
        return (num / 3.0) * 100;
    }

    // Function for column 19: Gender
    public static double calculateGender(String input) {
        return 0;
    }

    // Function for column 20: Exam score
    public static double calculateExamScore(String input) {
        return 0;
    }

    // Helper functions for converting input values to numerical values
    private static int convertLowMediumHigh(String input) {
        if (input.equalsIgnoreCase("Low")) return 1;
        if (input.equalsIgnoreCase("Medium")) return 2;
        if (input.equalsIgnoreCase("High")) return 3;
        return 0;
    }

    private static int convertPeerInfluence(String input) {
        if (input.equalsIgnoreCase("Positive")) return 3;
        if (input.equalsIgnoreCase("Neutral")) return 2;
        if (input.equalsIgnoreCase("Negative")) return 1;
        return 0;
    }

    private static int convertParentEducation(String input) {
        if (input.equalsIgnoreCase("High School")) return 1;
        if (input.equalsIgnoreCase("College")) return 2;
        if (input.equalsIgnoreCase("Postgraduate")) return 3;
        return 0;
    }

    private static int convertDistance(String input) {
        if (input.equalsIgnoreCase("Far")) return 1;
        if (input.equalsIgnoreCase("Moderate")) return 2;
        if (input.equalsIgnoreCase("Near")) return 3;
        return 0;
    }

    // Mapper class
    public static class StudentMapper extends Mapper<Object, Text, IntWritable, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] columns = value.toString().split(",");
            int studentRowNum = Integer.parseInt(columns[0]);

            double totalScore = calculateTotalScore(columns);
            context.write(new IntWritable(studentRowNum), new Text(String.valueOf(totalScore)));
        }
    }

    // Reducer class
    public static class StudentReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        private static final double OPPORTUNITY_THRESHOLD = 60.0;

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double totalScore = 0.0;
            for (Text value : values) {
                totalScore += Double.parseDouble(value.toString());
            }

            String opportunityStatus = (totalScore >= OPPORTUNITY_THRESHOLD) ? "Using Opportunity" : "Not Using Opportunity";
            context.write(key, new Text("Total Score: " + totalScore + ", " + opportunityStatus));
        }
    }

    // Driver class
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: StudentOpportunityDriver <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Student Opportunity Utilization");

        job.setJarByClass(StudentOpportunityAnalysis.class);
        job.setMapperClass(StudentMapper.class);
        job.setReducerClass(StudentReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    // Function to calculate total score for a student
    public static double calculateTotalScore(String[] inputs) {
        double totalScore = 0;
        totalScore += calculateHoursStudied(inputs[1]);
        totalScore += calculateAttendance(inputs[2]);
        totalScore += calculateParentalInvolvement(inputs[3]);
        totalScore += calculateAccessToResources(inputs[4]);
        totalScore += calculateExtracurricular(inputs[5]);
        totalScore += calculateSleep(inputs[6]);
        totalScore += calculatePreviousScore(inputs[7]);
        totalScore += calculateMotivationLevel(inputs[8]);
        totalScore += calculateInternetAccess(inputs[9]);
        totalScore += calculateTutoringSession(inputs[10]);
        totalScore += calculateFamilyIncome(inputs[11]);
        totalScore += calculateTeacherQuality(inputs[12]);
        totalScore += calculateSchoolType(inputs[13]);
        totalScore += calculatePeerInfluence(inputs[14]);
        totalScore += calculatePhysicalActivity(inputs[15]);
        totalScore += calculateLearningDisabilities(inputs[16]);
        totalScore += calculateParentEducation(inputs[17]);
        totalScore += calculateDistanceFromSchool(inputs[18]);
        totalScore += calculateGender(inputs[19]);
        totalScore += calculateExamScore(inputs[20]);
        return totalScore;
    }
}
