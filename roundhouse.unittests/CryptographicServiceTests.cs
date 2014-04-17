using System;
using System.Diagnostics;
using System.Text;
using System.Security.Cryptography;
using NUnit.Framework;
using roundhouse.cryptography;

namespace roundhouse.unittests
{
    using System.Security.AccessControl;

    using roundhouse.infrastructure.filesystem;

    [TestFixture]
    public class CryptographicServiceTests
    {
        [Test]
        public void CreateMD5Cipher_WithString_GivesMeTheCorrectNumberOfBytes()
        {
            var md5crypto           = MD5.Create();
            var text_to_hash        = "I want to see what the freak is going on here";
            byte[] clear_text_bytes = Encoding.UTF8.GetBytes(text_to_hash);
            byte[] cypher_bytes     = md5crypto.ComputeHash(clear_text_bytes);
            Assert.AreEqual(16, cypher_bytes.Length);
            Debug.WriteLine(cypher_bytes);
            string base_64_cypher   = Convert.ToBase64String(cypher_bytes);
            Assert.AreEqual(24, base_64_cypher.Length);
            Debug.WriteLine(base_64_cypher);
        }

        [Test]
        public void Hash_WithString_ReturnsProperString()
        {
            var md5_crypto       = new MD5CryptographicService();
            string text_to_hash  = "I want to see what the freak is going on here";
            string expected_hash = "TMGPZJmBhSO5uYbf/TBqNA==";
            Assert.AreEqual(expected_hash, md5_crypto.hash(text_to_hash));
        }

        [Test]
        public void Add_Always_ReturnsTheSum()
        {
            var num1 = 1;
            var num2 = 2;
            var expected = 3;
            Assert.AreEqual(expected, num1 + num2);
        }
    }
}
