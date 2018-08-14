package data;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

public class PrepRawdata {

    public static JavaRDD<LabeledPoint> createRandForestData(JavaRDD<String[]> datasetIn) {
        // *************************
        // create training/test data
        // *************************

        JavaRDD<LabeledPoint> datasetOut = datasetIn.map(s -> {

            // 1 SeriousDlqin2yrs, 2 RevolvingUtilizationOfUnsecuredLines, 3 age, 4 NumberOfTime30-59DaysPastDueNotWorse
            // 5 DebtRatio, 6 MonthlyIncome, 7 NumberOfOpenCreditLinesAndLoans, 8 NumberOfTimes90DaysLate
            // 9 NumberRealEstateLoansOrLines, 10 NumberOfTime60-89DaysPastDueNotWorse, 11 NumberOfDependents

            int UnknownNumberOfDependents = 0;
            if (s[11].equals("NA")) {
                UnknownNumberOfDependents = 1;
            }

            int UnknownMonthlyIncome = 0 ;
            if(s[6].equals("NA")){UnknownMonthlyIncome = 1;};

            int NoDependents = 0;
            if(s[11].equals("0")){NoDependents = 1;};

            int NumberOfDependents;
            if(s[11].equals("0")){NumberOfDependents=0;}
            else if(s[11].equals("NA")){NumberOfDependents=0;}
            else{NumberOfDependents=Integer.parseInt(s[11]);}

            int NoIncome=0;
            if(s[6].equals("NA")){NoIncome=1;}
            else if(s[6].equals("0")){NoIncome=1;}

            double MonthlyIncome;
            if(UnknownMonthlyIncome == 1){MonthlyIncome = new Double(0);}
            else{MonthlyIncome = Double.parseDouble(s[6]);}

            int ZeroDebtRatio=0;
            if(s[5].equals("0")){ZeroDebtRatio=1;};

            double UnknownIncomeDebtRatio;
            if(UnknownMonthlyIncome == 0){UnknownIncomeDebtRatio = new Double(0);}
            else{UnknownIncomeDebtRatio = Double.parseDouble(s[5]);};

            double DebtRatio;
            if(UnknownMonthlyIncome == 1){DebtRatio = new Double(0);}
            else{DebtRatio = Double.parseDouble(s[5]);}

            double WeirdRevolvingUtilization = new Float(0);
            if (Math.log(Double.parseDouble(s[2])) > 3){WeirdRevolvingUtilization = Double.parseDouble(s[2]);}

            int ZeroRevolvingUtilization=0;
            if (s[2].equals("0")){ZeroRevolvingUtilization=1;}

            double RevolvingUtilizationOfUnsecuredLines = new Float(0);
            if (Math.log(Double.parseDouble(s[2])) <= 3){RevolvingUtilizationOfUnsecuredLines = Double.parseDouble(s[2]);}

            double LogDebt;
            if(MonthlyIncome <= 1){LogDebt=Math.log(DebtRatio);}
            else{LogDebt=Math.log(MonthlyIncome * DebtRatio);}

            double RevolvingLines = Double.parseDouble(s[7]) - Double.parseDouble(s[9]);

            int HasRevolvingLines = 0;
            if(RevolvingLines > 0){HasRevolvingLines=1;}

            int HasRealEstateLoans = 0;
            if(Integer.parseInt(s[9]) > 0){HasRealEstateLoans = 1;};

            int HasMultipleRealEstateLoans = 0;
            if(Integer.parseInt(s[9]) > 2){HasMultipleRealEstateLoans = 1;};

            int EligibleSS = 0;
            if(Integer.parseInt(s[3]) > 60){EligibleSS = 1;};

            int DTIOver33 = 0;
            if(NoIncome == 0 && DebtRatio > 0.33){DTIOver33 = 1;};

            int DTIOver43 = 0;
            if(NoIncome == 0 && DebtRatio > 0.43){EligibleSS = 1;};

            double DisposableIncome = new Double(0);
            if(NoIncome!=1){DisposableIncome = ((1 - DebtRatio) * MonthlyIncome);};

            double RevolvingToRealEstate = RevolvingLines / (1 + Integer.parseInt(s[9]));

            int NumberOfTime3059DaysPastDueNotWorseLarge = 0;
            if(Double.parseDouble(s[4]) > 90){NumberOfTime3059DaysPastDueNotWorseLarge = 1;};

            int NumberOfTime3059DaysPastDueNotWorse96 = 0;
            if(Double.parseDouble(s[4]) == 96){NumberOfTime3059DaysPastDueNotWorse96 = 1;};

            int NumberOfTime3059DaysPastDueNotWorse98 = 0;
            if(Double.parseDouble(s[4]) == 98){NumberOfTime3059DaysPastDueNotWorse98 = 1;};

            int Never3059DaysPastDueNotWorse = 0;
            if(Double.parseDouble(s[4]) == 0){Never3059DaysPastDueNotWorse = 1;};

            int NumberOfTime3059DaysPastDueNotWorse = 0;
            if(Double.parseDouble(s[4]) <= 90){NumberOfTime3059DaysPastDueNotWorse = Integer.parseInt(s[4]);};

            int NumberOfTime6089DaysPastDueNotWorseLarge = 0;
            if(Double.parseDouble(s[10]) > 90){NumberOfTime6089DaysPastDueNotWorseLarge = 1;};

            int NumberOfTime6089DaysPastDueNotWorse96 = 0;
            if(Double.parseDouble(s[10]) == 96){NumberOfTime6089DaysPastDueNotWorse96 = 1;};

            int NumberOfTime6089DaysPastDueNotWorse98 = 0;
            if(Double.parseDouble(s[10]) == 98){NumberOfTime6089DaysPastDueNotWorse98 = 1;};

            int Never6089DaysPastDueNotWorse = 0;
            if(Double.parseDouble(s[10]) == 0){Never6089DaysPastDueNotWorse = 1;};

            int NumberOfTime6089DaysPastDueNotWorse = 0;
            if(Double.parseDouble(s[10]) <= 90){NumberOfTime6089DaysPastDueNotWorse = Integer.parseInt(s[10]);};

            int NumberOfTimes90DaysLateLarge = 0;
            if(Double.parseDouble(s[8]) > 90){NumberOfTime6089DaysPastDueNotWorse = 1;};

            int NumberOfTimes90DaysLate96 = 0;
            if(Double.parseDouble(s[8]) == 96){NumberOfTimes90DaysLate96 = 1;};

            int NumberOfTimes90DaysLate98 = 0;
            if(Double.parseDouble(s[8]) == 98){NumberOfTimes90DaysLate98 = 1;};

            int Never90DaysLate = 0;
            if(Double.parseDouble(s[8]) == 0){Never90DaysLate = 1;};

            int NumberOfTimes90DaysLate = 0;
            if(Double.parseDouble(s[8]) <= 90){NumberOfTimes90DaysLate = Integer.parseInt(s[8]);};

            int IncomeDivBy10 = 0;
            if ((MonthlyIncome % 10) == 0){IncomeDivBy10 = 1;};

            int IncomeDivBy100 = 0;
            if ((MonthlyIncome % 100) == 0){IncomeDivBy100 = 1;};

            int IncomeDivBy1000 = 0;
            if ((MonthlyIncome % 1000) == 0){IncomeDivBy1000 = 1;};

            int IncomeDivBy5000 = 0;
            if ((MonthlyIncome % 5000) == 0){IncomeDivBy5000 = 1;};

            int Weird0999Utilization = 0;
            if(RevolvingUtilizationOfUnsecuredLines == 0.9999999){Weird0999Utilization = 1;};

            int FullUtilization = 0;
            if(RevolvingUtilizationOfUnsecuredLines == 1){FullUtilization = 1;};

            int ExcessUtilization=0;
            if(RevolvingUtilizationOfUnsecuredLines > 1){ExcessUtilization=1;};

            int NumberOfTime3089DaysPastDueNotWorse = NumberOfTime3059DaysPastDueNotWorse + NumberOfTime6089DaysPastDueNotWorse;

            int Never3089DaysPastDueNotWorse = Never6089DaysPastDueNotWorse * Never3059DaysPastDueNotWorse;

            int NumberOfTimesPastDue = NumberOfTime3059DaysPastDueNotWorse + NumberOfTime6089DaysPastDueNotWorse + NumberOfTimes90DaysLate;

            int NeverPastDue = Never90DaysLate * Never6089DaysPastDueNotWorse * Never3059DaysPastDueNotWorse;

            double LogRevolvingUtilizationTimesLines = Math.log1p(RevolvingLines * RevolvingUtilizationOfUnsecuredLines);
            if(Double.isNaN(LogRevolvingUtilizationTimesLines) | Double.isInfinite(LogRevolvingUtilizationTimesLines)){
                LogRevolvingUtilizationTimesLines = 0;
            }

            double LogRevolvingUtilizationOfUnsecuredLines = Math.log(RevolvingUtilizationOfUnsecuredLines);
            if(Double.isNaN(LogRevolvingUtilizationOfUnsecuredLines) | Double.isInfinite(LogRevolvingUtilizationOfUnsecuredLines)){
                LogRevolvingUtilizationOfUnsecuredLines = 0;
            }

            double DelinquenciesPerLine = NumberOfTimesPastDue / Double.parseDouble(s[7]);
            if(Double.isNaN(DelinquenciesPerLine) | Double.isInfinite(DelinquenciesPerLine)){
                DelinquenciesPerLine = 0;
            }

            double MajorDelinquenciesPerLine = NumberOfTimes90DaysLate / Double.parseDouble(s[7]);
            if(Double.isNaN(MajorDelinquenciesPerLine) | Double.isInfinite(MajorDelinquenciesPerLine)){
                MajorDelinquenciesPerLine = 0;
            }

            double MinorDelinquenciesPerLine = NumberOfTime3089DaysPastDueNotWorse / Double.parseDouble(s[7]);
            if(Double.isNaN(MinorDelinquenciesPerLine) | Double.isInfinite(MinorDelinquenciesPerLine)){
                MinorDelinquenciesPerLine = 0;
            }

            double DelinquenciesPerRevolvingLine = NumberOfTimesPastDue / RevolvingLines;
            if(Double.isNaN(DelinquenciesPerRevolvingLine) | Double.isInfinite(DelinquenciesPerRevolvingLine)){
                DelinquenciesPerRevolvingLine = 0;
            }

            double MajorDelinquenciesPerRevolvingLine = NumberOfTimes90DaysLate / RevolvingLines;
            if(Double.isNaN(MajorDelinquenciesPerRevolvingLine) | Double.isInfinite(MajorDelinquenciesPerRevolvingLine)){
                MajorDelinquenciesPerRevolvingLine = 0;
            }

            double MinorDelinquenciesPerRevolvingLine = NumberOfTime3089DaysPastDueNotWorse / RevolvingLines;
            if(Double.isNaN(MinorDelinquenciesPerRevolvingLine) | Double.isInfinite(MinorDelinquenciesPerRevolvingLine)){
                MinorDelinquenciesPerRevolvingLine = 0;
            }

            double LogDebtPerLine = LogDebt - Math.log1p(Double.parseDouble(s[7]));
            if(Double.isNaN(LogDebtPerLine) | Double.isInfinite(LogDebtPerLine)){
                LogDebtPerLine = 0;
            }

            double LogDebtPerRealEstateLine = LogDebt - Math.log1p(Double.parseDouble(s[9]));
            if(Double.isNaN(LogDebtPerRealEstateLine) | Double.isInfinite(LogDebtPerRealEstateLine)){
                LogDebtPerRealEstateLine = 0;
            }

            double LogDebtPerPerson = LogDebt - Math.log1p(NumberOfDependents);
            if(Double.isNaN(LogDebtPerPerson) | Double.isInfinite(LogDebtPerPerson)){
                LogDebtPerPerson = 0;
            }

            double RevolvingLinesPerPerson = RevolvingLines / ( 1+NumberOfDependents);
            if(Double.isNaN(RevolvingLinesPerPerson) | Double.isInfinite(RevolvingLinesPerPerson)){
                RevolvingLinesPerPerson = 0;
            }

            double RealEstateLoansPerPerson = Double.parseDouble(s[9]) / ( 1+NumberOfDependents);
            if(Double.isNaN(RealEstateLoansPerPerson) | Double.isInfinite(RealEstateLoansPerPerson)){
                RealEstateLoansPerPerson = 0;
            }

            double YearsOfAgePerDependent = Double.parseDouble(s[3]) / ( 1+NumberOfDependents);
            if(Double.isNaN(YearsOfAgePerDependent) | Double.isInfinite(YearsOfAgePerDependent)){
                YearsOfAgePerDependent = 0;
            }

            double LogMonthlyIncome = Math.log1p(MonthlyIncome);
            if(Double.isNaN(LogMonthlyIncome) | Double.isInfinite(LogMonthlyIncome)){
                LogMonthlyIncome = 0;
            }

            double LogIncomePerPerson = LogMonthlyIncome - Math.log1p(NumberOfDependents);
            if(Double.isNaN(LogIncomePerPerson) | Double.isInfinite(LogIncomePerPerson)){
                LogIncomePerPerson = 0;
            }

            double LogIncomeAge = LogMonthlyIncome - Math.log1p(Double.parseDouble(s[3]));
            if(Double.isNaN(LogIncomeAge) | Double.isInfinite(LogIncomeAge)){
                LogIncomeAge = 0;
            }

            double LogNumberOfTimesPastDue = Math.log(NumberOfTimesPastDue);
            if(Double.isNaN(LogNumberOfTimesPastDue) | Double.isInfinite(LogNumberOfTimesPastDue)){
                LogNumberOfTimesPastDue = 0;
            }

            double LogNumberOfTimes90DaysLate = Math.log(NumberOfTimes90DaysLate);
            if(Double.isNaN(LogNumberOfTimes90DaysLate) | Double.isInfinite(LogNumberOfTimes90DaysLate)){
                LogNumberOfTimes90DaysLate = 0;
            }

            double LogNumberOfTime3059DaysPastDueNotWorse = Math.log(NumberOfTime3059DaysPastDueNotWorse);
            if(Double.isNaN(LogNumberOfTime3059DaysPastDueNotWorse) | Double.isInfinite(LogNumberOfTime3059DaysPastDueNotWorse)){
                LogNumberOfTime3059DaysPastDueNotWorse = 0;
            }

            double LogNumberOfTime6089DaysPastDueNotWorse = Math.log(NumberOfTime6089DaysPastDueNotWorse);
            if(Double.isNaN(LogNumberOfTime6089DaysPastDueNotWorse) | Double.isInfinite(LogNumberOfTime6089DaysPastDueNotWorse)){
                LogNumberOfTime6089DaysPastDueNotWorse = 0;
            }

            double LogRatio90to3059DaysLate = LogNumberOfTimes90DaysLate - LogNumberOfTime3059DaysPastDueNotWorse;
            if(Double.isNaN(LogRatio90to3059DaysLate) | Double.isInfinite(LogRatio90to3059DaysLate)){
                LogRatio90to3059DaysLate = 0;
            }

            double LogRatio90to6089DaysLate = LogNumberOfTimes90DaysLate - LogNumberOfTime6089DaysPastDueNotWorse;
            if(Double.isNaN(LogRatio90to6089DaysLate) | Double.isInfinite(LogRatio90to6089DaysLate)){
                LogRatio90to6089DaysLate = 0;
            }

            int AnyOpenCreditLinesOrLoans = 0;
            if(Integer.parseInt(s[7]) > 0){AnyOpenCreditLinesOrLoans = 1;};

            double LogNumberOfOpenCreditLinesAndLoans = Math.log(Double.parseDouble(s[7]));
            if(Double.isNaN(LogNumberOfOpenCreditLinesAndLoans) | Double.isInfinite(LogNumberOfOpenCreditLinesAndLoans)){
                LogNumberOfOpenCreditLinesAndLoans = 0;
            }

            double LogNumberOfOpenCreditLinesAndLoansPerPerson = LogNumberOfOpenCreditLinesAndLoans - Math.log1p(NumberOfDependents);
            if(Double.isNaN(LogNumberOfOpenCreditLinesAndLoansPerPerson) | Double.isInfinite(LogNumberOfOpenCreditLinesAndLoansPerPerson)){
                LogNumberOfOpenCreditLinesAndLoansPerPerson = 0;
            }

            int HasDependents = 0;
            if(NumberOfDependents > 0){HasDependents = 1;};

            double LogHouseholdSize = Math.log1p(NumberOfDependents);
            if(Double.isNaN(LogHouseholdSize) | Double.isInfinite(LogHouseholdSize)){
                LogHouseholdSize = 0;
            }

            double LogDebtRatio = Math.log1p(DebtRatio);
            if(Double.isNaN(LogDebtRatio) | Double.isInfinite(LogDebtRatio)){
                LogDebtRatio = 0;
            }

            double LogUnknownIncomeDebtRatio = Math.log(UnknownIncomeDebtRatio);
            if(Double.isNaN(LogUnknownIncomeDebtRatio) | Double.isInfinite(LogUnknownIncomeDebtRatio)){
                LogUnknownIncomeDebtRatio = 0;
            }

            double LogUnknownIncomeDebtRatioPerPerson = LogUnknownIncomeDebtRatio - LogHouseholdSize;

            double LogNumberRealEstateLoansOrLines = Math.log(Double.parseDouble(s[7]));
            if(Double.isNaN(LogNumberRealEstateLoansOrLines) | Double.isInfinite(LogNumberRealEstateLoansOrLines)){
                LogNumberRealEstateLoansOrLines = 0;
            }

            int LowAge = 0; if(Integer.parseInt(s[3]) < 18){LowAge = 1;};

            double Logage;
            if(LowAge == 1){Logage = 0;}
            else{Logage = Math.log(Integer.parseInt(s[3]) - 17);};
            if(Double.isNaN(Logage) | Double.isInfinite(Logage)){
                Logage = 0;
            }

            // Extra variables to test
            double LogDebtPerDelinquency = LogDebt - Math.log1p(NumberOfTimesPastDue);
            if(Double.isNaN(LogDebtPerDelinquency) | Double.isInfinite(LogDebtPerDelinquency)){
                LogDebtPerDelinquency = 0;
            }

            double LogDebtPer90DaysLate = LogDebt - Math.log1p(NumberOfTimes90DaysLate);
            if(Double.isNaN(LogDebtPer90DaysLate) | Double.isInfinite(LogDebtPer90DaysLate)){
                LogDebtPer90DaysLate = 0;
            }

            double LogUnknownIncomeDebtRatioPerDelinquency = LogUnknownIncomeDebtRatio - Math.log1p(NumberOfTimesPastDue);
            if(Double.isNaN(LogUnknownIncomeDebtRatioPerDelinquency) | Double.isInfinite(LogUnknownIncomeDebtRatioPerDelinquency)){
                LogUnknownIncomeDebtRatioPerDelinquency = 0;
            }

            double LogUnknownIncomeDebtRatioPer90DaysLate = LogUnknownIncomeDebtRatio - Math.log1p(NumberOfTimes90DaysLate);
            if(Double.isNaN(LogUnknownIncomeDebtRatioPer90DaysLate) | Double.isInfinite(LogUnknownIncomeDebtRatioPer90DaysLate)){
                LogUnknownIncomeDebtRatioPer90DaysLate = 0;
            }

            Vector indepVars = Vectors.dense(
                    UnknownNumberOfDependents
                    , UnknownMonthlyIncome
                    , NoDependents
                    , NoIncome
                    , ZeroDebtRatio
                    , UnknownIncomeDebtRatio
                    , WeirdRevolvingUtilization
                    , ZeroRevolvingUtilization
                    , LogDebt
                    , RevolvingLines
                    , HasRevolvingLines
                    , HasRealEstateLoans
                    , HasMultipleRealEstateLoans
                    , EligibleSS
                    , DTIOver33
                    , DTIOver43
                    , DisposableIncome
                    , RevolvingToRealEstate
                    , NumberOfTime3059DaysPastDueNotWorseLarge
                    , NumberOfTime3059DaysPastDueNotWorse96
                    , NumberOfTime3059DaysPastDueNotWorse98
                    , Never3059DaysPastDueNotWorse
                    , NumberOfTime3059DaysPastDueNotWorse
                    , NumberOfTime6089DaysPastDueNotWorseLarge
                    , NumberOfTime6089DaysPastDueNotWorse96
                    , NumberOfTime6089DaysPastDueNotWorse98
                    , NumberOfTime6089DaysPastDueNotWorse
                    , NumberOfTimes90DaysLateLarge
                    , NumberOfTimes90DaysLate96
                    , NumberOfTimes90DaysLate98
                    , Never90DaysLate
                    , IncomeDivBy10
                    , IncomeDivBy100
                    , IncomeDivBy1000
                    , IncomeDivBy5000
                    , Weird0999Utilization
                    , FullUtilization
                    , ExcessUtilization
                    , NumberOfTime3089DaysPastDueNotWorse
                    , Never3089DaysPastDueNotWorse
                    , NeverPastDue
                    , LogRevolvingUtilizationTimesLines
                    , LogRevolvingUtilizationOfUnsecuredLines
                    , DelinquenciesPerLine
                    , MajorDelinquenciesPerLine
                    , MinorDelinquenciesPerLine
                    , DelinquenciesPerRevolvingLine
                    , MajorDelinquenciesPerRevolvingLine
                    , MinorDelinquenciesPerRevolvingLine
                    , LogDebtPerLine
                    , LogDebtPerRealEstateLine
                    , LogDebtPerPerson
                    , RevolvingLinesPerPerson
                    , RealEstateLoansPerPerson
                    , YearsOfAgePerDependent
                    , LogMonthlyIncome
                    , LogIncomePerPerson
                    , LogIncomeAge
                    , LogNumberOfTimesPastDue
                    , LogNumberOfTimes90DaysLate
                    , LogNumberOfTime3059DaysPastDueNotWorse
                    , LogNumberOfTime6089DaysPastDueNotWorse
                    , LogRatio90to3059DaysLate
                    , LogRatio90to6089DaysLate
                    , AnyOpenCreditLinesOrLoans
                    , LogNumberOfOpenCreditLinesAndLoans
                    , LogNumberOfOpenCreditLinesAndLoansPerPerson
                    , HasDependents
                    , LogHouseholdSize
                    , LogDebtRatio
                    , LogUnknownIncomeDebtRatio
                    , LogUnknownIncomeDebtRatioPerPerson
                    , LogNumberRealEstateLoansOrLines
                    , LowAge
                    , Logage
                    , LogDebtPerDelinquency
                    , LogDebtPer90DaysLate
                    , LogUnknownIncomeDebtRatioPerDelinquency
                    , LogUnknownIncomeDebtRatioPer90DaysLate
            );

            // dependent variable:
            double SeriousDlqin2yrs = Double.parseDouble(s[1]);

            // create labeled point
            LabeledPoint lsub = new LabeledPoint(
                    // label
                    SeriousDlqin2yrs
                    // features
                    , indepVars
            );
            return lsub;
        });
        return datasetOut;
    }
}


