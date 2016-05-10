#ifndef DISKFILEWRITER_H
#define DISKFILEWRITER_H


class DiskFileWriter {


private:
    // disallow copies
    DiskFileWriter(const DiskFileWriter&);
    DiskFileWriter& operator=(const DiskFileWriter&);


public:
    bool put_succeeded;


    DiskFileWriter() :
        put_succeeded(false) {
    }

};

#endif


