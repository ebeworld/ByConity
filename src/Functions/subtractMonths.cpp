#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionSubtractMonths = FunctionDateOrDateTimeAddInterval<SubtractMonthsImpl>;

void registerFunctionSubtractMonths(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSubtractMonths>(FunctionFactory::CaseInsensitive);
}

}


