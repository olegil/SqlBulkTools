using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Ploeh.AutoFixture;
using Ploeh.AutoFixture.Kernel;
using SqlBulkTools.IntegrationTests.Model;

namespace SqlBulkTools.IntegrationTests
{
#pragma warning disable 618
    public class BookRandomizer
    {

        public List<Book> GetRandomCollection(int count)
        {
            var fixture = new Fixture();
            fixture.Customizations.Add(new PriceBuilder());
            fixture.Customizations.Add(new IsbnBuilder());
            fixture.Customizations.Add(new TitleBuilder());
            List<Book> books = new List<Book>();
            books = fixture.Build<Book>().Without(x => x.Id).CreateMany(count).ToList();
            return books;
        }

    }

    public class PriceBuilder : ISpecimenBuilder
    {
        public object Create(object request, ISpecimenContext context)
        {
            var pi = request as PropertyInfo;
            if (pi == null ||
                pi.Name != "Price" ||
                pi.PropertyType != typeof(decimal))
                return new NoSpecimen(request);

            return context.Resolve(
                new RangedNumberRequest(typeof(decimal), 1.0m, 268.5m));
        }
    }

    public class IsbnBuilder : ISpecimenBuilder
    {
        public object Create(object request, ISpecimenContext context)
        {
            var pi = request as PropertyInfo;
            if (pi != null &&
                pi.Name == "ISBN" &&
                pi.PropertyType == typeof(string))

            return context.Resolve(typeof(string))
                    .ToString().Substring(0, 13);

            return new NoSpecimen(request);
        }
    }

    public class TitleBuilder : ISpecimenBuilder
    {
        public object Create(object request, ISpecimenContext context)
        {
            var pi = request as PropertyInfo;
            if (pi != null &&
                pi.Name == "Title" &&
                pi.PropertyType == typeof(string))

                return context.Resolve(typeof(string))
                        .ToString().Substring(0, 10);

            return new NoSpecimen(request);
        }
    }
}
