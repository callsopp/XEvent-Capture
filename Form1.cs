using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.Threading;

namespace XEventCapture
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }

        private void createVirtualStreamBT_Click(object sender, EventArgs e)
        {
            //var t = Task.Run(() => { new ExtendedEventStreamCapture("mr-pc",textBox1.Text, textBox2.Text); });
            var t = Task.Run(() => { new ExtendedEventCaptureMonitor("MR-PC"); });
            //ExtendedEventStreamCapture x = new ExtendedEventStreamCapture(textBox1.Text, textBox2.Text);
            //x.AttachStream();
        }   
    }
}
