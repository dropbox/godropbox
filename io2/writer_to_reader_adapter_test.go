package io2

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"io"
	"strings"

	. "gopkg.in/check.v1"
)

type WriterToReaderAdapterSuite struct {
	testData string
}

var words = []string{
	`Once`, `upon`, `a`, `midnight`, `dreary,`, `while`,
	`I`, `pondered,`, `weak`, `and`, `weary,`, ``,
	`Over`, `many`, `a`, `quaint`, `and`, `curious`,
	`volume`, `of`, `forgotten`, `lore,`, ``,
	`While`, `I`, `nodded,`, `nearly`, `napping,`,
	`suddenly`, `there`, `came`, `a`, `tapping,`, ``,
	`As`, `of`, `some`, `one`, `gently`, `rapping,`,
	`rapping`, `at`, `my`, `chamber`, `door.`, ``,
	`"'Tis`, `some`, `visitor,"`, `I`, `muttered,`,
	`"tapping`, `at`, `my`, `chamber`, `door-`, ``,
	``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``,
	``, ``, ``, ``, `Only`, `this,`, `and`, `nothing`, `more."`, ``,
	`Ah,`, `distinctly`, `I`, `remember`, `it`,
	`was`, `in`, `the`, `bleak`, `December,`, ``,
	`And`, `each`, `separate`, `dying`, `ember`,
	`wrought`, `its`, `ghost`, `upon`, `the`, `floor.`, ``,
	`Eagerly`, `I`, `wished`, `the`, `morrow;-`,
	`vainly`, `I`, `had`, `sought`, `to`, `borrow`, ``,
	`From`, `my`, `books`, `surcease`, `of`,
	`sorrow-`, `sorrow`, `for`, `the`, `lost`, `Lenore-`, ``,
	`For`, `the`, `rare`, `and`, `radiant`,
	`maiden`, `whom`, `the`, `angels`, `name`, `Lenore-`, ``,
	``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``,
	``, ``, `Nameless`, `here`, `for`, `evermore.`, ``,
	`And`, `the`, `silken,`, `sad,`, `uncertain`, `rustling`,
	`of`, `each`, `purple`, `curtain`, ``,
	`Thrilled`, `me-`, `filled`, `me`, `with`, `fantastic`,
	`terrors`, `never`, `felt`, `before;`, ``,
	`So`, `that`, `now,`, `to`, `still`, `the`, `beating`,
	`of`, `my`, `heart,`, `I`, `stood`, `repeating,`, ``,
	`"'Tis`, `some`, `visitor`, `entreating`, `entrance`,
	`at`, `my`, `chamber`, `door-`, ``,
	`Some`, `late`, `visitor`, `entreating`, `entrance`,
	`at`, `my`, `chamber`, `door;-`, ``,
	``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``,
	``, ``, ``, `This`, `it`, `is,`, `and`, `nothing`, `more."`, ``,
	`Presently`, `my`, `soul`, `grew`, `stronger;`,
	`hesitating`, `then`, `no`, `longer,`, ``,
	`"Sir,"`, `said`, `I,`, `"or`, `Madam,`, `truly`,
	`your`, `forgiveness`, `I`, `implore;`, ``,
	`But`, `the`, `fact`, `is`, `I`, `was`, `napping,`,
	`and`, `so`, `gently`, `you`, `came`, `rapping,`, ``,
	`And`, `so`, `faintly`, `you`, `came`, `tapping,`,
	`tapping`, `at`, `my`, `chamber`, `door,`, ``,
	`That`, `I`, `scarce`, `was`, `sure`, `I`, `heard`,
	`you"-`, `here`, `I`, `opened`, `wide`, `the`, `door;-`, ``,
	``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``,
	``, ``, ``, `Darkness`, `there,`, `and`, `nothing`, `more.`, ``,
	`Deep`, `into`, `that`, `darkness`, `peering,`, `long`,
	`I`, `stood`, `there`, `wondering,`, `fearing,`, ``,
	`Doubting,`, `dreaming`, `dreams`, `no`, `mortal`, `ever`,
	`dared`, `to`, `dream`, `before;`, ``,
	`But`, `the`, `silence`, `was`, `unbroken,`, `and`, `the`,
	`stillness`, `gave`, `no`, `token,`, ``,
	`And`, `the`, `only`, `word`, `there`, `spoken`, `was`,
	`the`, `whispered`, `word,`, `"Lenore?"`, ``,
	`This`, `I`, `whispered,`, `and`, `an`, `echo`, `murmured`,
	`back`, `the`, `word,`, `"Lenore!"-`, ``,
	``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``,
	``, `Merely`, `this,`, `and`, `nothing`, `more.`, ``,
	`Back`, `into`, `the`, `chamber`, `turning,`, `all`,
	`my`, `soul`, `within`, `me`, `burning,`, ``,
	`Soon`, `again`, `I`, `heard`, `a`, `tapping`,
	`somewhat`, `louder`, `than`, `before.`, ``,
	`"Surely,"`, `said`, `I,`, `"surely`, `that`, `is`,
	`something`, `at`, `my`, `window`, `lattice:`, ``,
	`Let`, `me`, `see,`, `then,`, `what`, `thereat`,
	`is,`, `and`, `this`, `mystery`, `explore-`, ``,
	`Let`, `my`, `heart`, `be`, `still`, `a`, `moment`,
	`and`, `this`, `mystery`, `explore;-`, ``,
	``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``,
	``, ``, ``, `'Tis`, `the`, `wind`, `and`, `nothing`, `more!"`, ``,
	`Open`, `here`, `I`, `flung`, `the`, `shutter,`, `when,`,
	`with`, `many`, `a`, `flirt`, `and`, `flutter,`, ``,
	`In`, `there`, `stepped`, `a`, `stately`, `Raven`, `of`,
	`the`, `saintly`, `days`, `of`, `yore;`, ``,
	`Not`, `the`, `least`, `obeisance`, `made`, `he;`, `not`,
	`a`, `minute`, `stopped`, `or`, `stayed`, `he;`, ``,
	`But,`, `with`, `mien`, `of`, `lord`, `or`, `lady,`, `perched`,
	`above`, `my`, `chamber`, `door-`, ``,
	`Perched`, `upon`, `a`, `bust`, `of`, `Pallas`, `just`, `above`,
	`my`, `chamber`, `door-`, ``,
	``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``,
	`Perched,`, `and`, `sat,`, `and`, `nothing`, `more.`, ``,
	`Then`, `this`, `ebony`, `bird`, `beguiling`, `my`, `sad`,
	`fancy`, `into`, `smiling,`, ``,
	`By`, `the`, `grave`, `and`, `stern`, `decorum`, `of`, `the`,
	`countenance`, `it`, `wore.`, ``,
	`"Though`, `thy`, `crest`, `be`, `shorn`, `and`, `shaven,`,
	`thou,"`, `I`, `said,`, `"art`, `sure`, `no`, `craven,`, ``,
	`Ghastly`, `grim`, `and`, `ancient`, `Raven`, `wandering`,
	`from`, `the`, `Nightly`, `shore-`, ``,
	`Tell`, `me`, `what`, `thy`, `lordly`, `name`, `is`, `on`,
	`the`, `Night's`, `Plutonian`, `shore!"`, ``,
	``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``,
	``, `Quoth`, `the`, `Raven,`, `"Nevermore."`, ``,
	`Much`, `I`, `marvelled`, `this`, `ungainly`, `fowl`, `to`,
	`hear`, `discourse`, `so`, `plainly,`, ``,
	`Though`, `its`, `answer`, `little`, `meaning-`,
	`little`, `relevancy`, `bore;`, ``,
	`For`, `we`, `cannot`, `help`, `agreeing`, `that`,
	`no`, `living`, `human`, `being`, ``,
	`Ever`, `yet`, `was`, `blessed`, `with`, `seeing`,
	`bird`, `above`, `his`, `chamber`, `door-`, ``,
	`Bird`, `or`, `beast`, `upon`, `the`, `sculptured`,
	`bust`, `above`, `his`, `chamber`, `door,`, ``,
	``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``,
	``, ``, ``, `With`, `such`, `name`, `as`, `"Nevermore."`, ``,
	`But`, `the`, `Raven,`, `sitting`, `lonely`, `on`,
	`the`, `placid`, `bust,`, `spoke`, `only`, ``,
	`That`, `one`, `word,`, `as`, `if`, `his`, `soul`,
	`in`, `that`, `one`, `word`, `he`, `did`, `outpour.`, ``,
	`Nothing`, `further`, `then`, `he`, `uttered-`, `not`,
	`a`, `feather`, `then`, `he`, `fluttered-`, ``,
	`Till`, `I`, `scarcely`, `more`, `than`, `muttered,`,
	`"Other`, `friends`, `have`, `flown`, `before-`, ``,
	`On`, `the`, `morrow`, `he`, `will`, `leave`, `me,`,
	`as`, `my`, `hopes`, `have`, `flown`, `before."`, ``,
	``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``,
	``, ``, `Then`, `the`, `bird`, `said,`, `"Nevermore."`, ``,
	`Startled`, `at`, `the`, `stillness`, `broken`, `by`,
	`reply`, `so`, `aptly`, `spoken,`, ``,
	`"Doubtless,"`, `said`, `I,`, `"what`, `it`, `utters`,
	`is`, `its`, `only`, `stock`, `and`, `store,`, ``,
	`Caught`, `from`, `some`, `unhappy`, `master`, `whom`,
	`unmerciful`, `Disaster`, ``,
	`Followed`, `fast`, `and`, `followed`, `faster`, `till`,
	`his`, `songs`, `one`, `burden`, `bore-`, ``,
	`Till`, `the`, `dirges`, `of`, `his`, `Hope`, `that`,
	`melancholy`, `burden`, `bore`, ``,
	``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``,
	`Of`, `'Never-`, `nevermore'."`, ``,
	`But`, `the`, `Raven`, `still`, `beguiling`, `all`, `my`,
	`fancy`, `into`, `smiling,`, ``,
	`Straight`, `I`, `wheeled`, `a`, `cushioned`, `seat`, `in`,
	`front`, `of`, `bird,`, `and`, `bust`, `and`, `door;`, ``,
	`Then`, `upon`, `the`, `velvet`, `sinking,`, `I`, `betook`,
	`myself`, `to`, `linking`, ``,
	`Fancy`, `unto`, `fancy,`, `thinking`, `what`, `this`,
	`ominous`, `bird`, `of`, `yore-`, ``,
	`What`, `this`, `grim,`, `ungainly,`, `ghastly,`,
	`gaunt`, `and`, `ominous`, `bird`, `of`, `yore`, ``,
	``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``,
	`Meant`, `in`, `croaking`, `"Nevermore."`, ``,
	`This`, `I`, `sat`, `engaged`, `in`, `guessing,`, `but`,
	`no`, `syllable`, `expressing`, ``,
	`To`, `the`, `fowl`, `whose`, `fiery`, `eyes`, `now`,
	`burned`, `into`, `my`, `bosom's`, `core;`, ``,
	`This`, `and`, `more`, `I`, `sat`, `divining,`, `with`,
	`my`, `head`, `at`, `ease`, `reclining`, ``,
	`On`, `the`, `cushion's`, `velvet`, `lining`, `that`,
	`the`, `lamp-light`, `gloated`, `o'er,`, ``,
	`But`, `whose`, `velvet`, `violet`, `lining`, `with`,
	`the`, `lamp-light`, `gloating`, `o'er,`, ``,
	``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``,
	`She`, `shall`, `press,`, `ah,`, `nevermore!`, ``,
	`Then`, `methought`, `the`, `air`, `grew`, `denser,`, `perfumed`,
	`from`, `an`, `unseen`, `censer`, ``,
	`Swung`, `by`, `Seraphim`, `whose`, `footfalls`, `tinkled`, `on`,
	`the`, `tufted`, `floor.`, ``,
	`"Wretch,"`, `I`, `cried,`, `"thy`, `God`, `hath`, `lent`, `thee-`,
	`by`, `these`, `angels`, `he`, `hath`, `sent`, `thee`, ``,
	`Respite-`, `respite`, `and`, `nepenthe,`, `from`, `thy`,
	`memories`, `of`, `Lenore!`, ``,
	`Quaff,`, `oh`, `quaff`, `this`, `kind`, `nepenthe`, `and`,
	`forget`, `this`, `lost`, `Lenore!"`, ``,
	``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``,
	`Quoth`, `the`, `Raven,`, `"Nevermore."`, ``,
	`"Prophet!"`, `said`, `I,`, `"thing`, `of`, `evil!`, `-`, `prophet`,
	`still,`, `if`, `bird`, `or`, `devil!`, `-`, ``,
	`Whether`, `Tempter`, `sent,`, `or`, `whether`, `tempest`,
	`tossed`, `thee`, `here`, `ashore,`, ``,
	`Desolate`, `yet`, `all`, `undaunted,`, `on`, `this`,
	`desert`, `land`, `enchanted-`, ``,
	`On`, `this`, `home`, `by`, `Horror`, `haunted-`, `tell`,
	`me`, `truly,`, `I`, `implore-`, ``,
	`Is`, `there-`, `is`, `there`, `balm`, `in`, `Gilead?-`, `tell`,
	`me-`, `tell`, `me,`, `I`, `implore!"`, ``,
	``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``,
	`Quoth`, `the`, `Raven,`, `"Nevermore."`, ``,
	`"Prophet!"`, `said`, `I,`, `"thing`, `of`, `evil!`, `-`, `prophet`,
	`still,`, `if`, `bird`, `or`, `devil!`, ``,
	`By`, `that`, `Heaven`, `that`, `bends`, `above`, `us-`, `by`,
	`that`, `God`, `we`, `both`, `adore-`, ``,
	`Tell`, `this`, `soul`, `with`, `sorrow`, `laden`, `if,`,
	`within`, `the`, `distant`, `Aidenn,`, ``,
	`It`, `shall`, `clasp`, `a`, `sainted`, `maiden`, `whom`,
	`the`, `angels`, `name`, `Lenore-`, ``,
	`Clasp`, `a`, `rare`, `and`, `radiant`, `maiden`, `whom`,
	`the`, `angels`, `name`, `Lenore."`, ``,
	``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``,
	`Quoth`, `the`, `Raven,`, `"Nevermore."`, ``,
	`"Be`, `that`, `word`, `our`, `sign`, `in`, `parting,`,
	`bird`, `or`, `fiend,"`, `I`, `shrieked,`, `upstarting-`, ``,
	`"Get`, `thee`, `back`, `into`, `the`, `tempest`,
	`and`, `the`, `Night's`, `Plutonian`, `shore!`, ``,
	`Leave`, `no`, `black`, `plume`, `as`, `a`, `token`,
	`of`, `that`, `lie`, `thy`, `soul`, `hath`, `spoken!`, ``,
	`Leave`, `my`, `loneliness`, `unbroken!-`, `quit`, `the`,
	`bust`, `above`, `my`, `door!`, ``,
	`Take`, `thy`, `beak`, `from`, `out`, `my`, `heart,`,
	`and`, `take`, `thy`, `form`, `from`, `off`, `my`, `door!"`, ``,
	``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``,
	`Quoth`, `the`, `Raven,`, `"Nevermore."`, ``,
	`And`, `the`, `Raven,`, `never`, `flitting,`, `still`,
	`is`, `sitting,`, `still`, `is`, `sitting`, ``,
	`On`, `the`, `pallid`, `bust`, `of`, `Pallas`, `just`,
	`above`, `my`, `chamber`, `door;`, ``,
	`And`, `his`, `eyes`, `have`, `all`, `the`, `seeming`,
	`of`, `a`, `demon's`, `that`, `is`, `dreaming,`, ``,
	`And`, `the`, `lamp-light`, `o'er`, `him`, `streaming`,
	`throws`, `his`, `shadow`, `on`, `the`, `floor;`, ``,
	`And`, `my`, `soul`, `from`, `out`, `that`, `shadow`,
	`that`, `lies`, `floating`, `on`, `the`, `floor`, ``,
	``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``, ``,
	``, ``, `Shall`, `be`, `lifted-`, `nevermore!`}

func (wrself *WriterToReaderAdapterSuite) SetUpTest(c *C) {
	localWords := []string{}
	for skip := 1; skip < len(words); skip += 6 {
		lskip := skip
		for i := 0; i < len(words); i += (lskip % 16) {
			localWords = append(localWords,
				words[i]+string([]byte{byte(i*len(words)+skip)%32 + byte('a')}))
			if lskip > 1 {
				lskip -= 1
			}
		}
	}
	wrself.testData = strings.Join(localWords, " _ ")

}

func (wrself *WriterToReaderAdapterSuite) TestZlibTwicePipeline(c *C) {
	var compressedInput bytes.Buffer
	w := zlib.NewWriter(&compressedInput)
	_, _ = w.Write([]byte(wrself.testData))
	_ = w.Close()
	var finalOutput bytes.Buffer
	finalDecompressor := NewWriterToReaderAdapter(
		func(a io.Reader) (io.Reader, error) { return zlib.NewReader(a) },
		&finalOutput, true)
	recompressor := zlib.NewWriter(finalDecompressor)
	initialWriteCloser := NewWriterToReaderAdapter(
		func(a io.Reader) (io.Reader, error) { return zlib.NewReader(a) },
		recompressor, true)
	skip := 1
	ci := compressedInput.Bytes()
	for len(ci) > 0 {
		if skip > len(ci) {
			skip = len(ci)
		}
		toCopy := ci[:skip]
		ci = ci[skip:]
		_, err := initialWriteCloser.Write(toCopy)
		c.Assert(err, IsNil)
		skip += 1
	}
	err := initialWriteCloser.Close()
	c.Assert(err, IsNil)
	//don't recompressor.Close()  // initialWriteCloser should close the downstream
	err = finalDecompressor.Close()
	c.Assert(err, IsNil)
	c.Assert(string(finalOutput.Bytes()), Equals, wrself.testData)
}

func (wrself *WriterToReaderAdapterSuite) TestZlibPipeline(c *C) {
	var compressedInput bytes.Buffer
	w := zlib.NewWriter(&compressedInput)
	_, _ = w.Write([]byte(wrself.testData))
	_ = w.Close()
	var finalOutput bytes.Buffer
	finalDecompressor := NewWriterToReaderAdapter(
		func(a io.Reader) (io.Reader, error) { return zlib.NewReader(a) },
		&finalOutput, true)
	skip := 1
	ci := compressedInput.Bytes()
	for len(ci) > 0 {
		if skip > len(ci) {
			skip = len(ci)
		}
		toCopy := ci[:skip]
		ci = ci[skip:]
		_, err := finalDecompressor.Write(toCopy)
		c.Assert(err, IsNil)
		skip %= 128
		skip += 1
	}

	err := finalDecompressor.Close()
	c.Assert(err, IsNil)
	c.Assert(string(finalOutput.Bytes()), Equals, wrself.testData)
}

func (wrself *WriterToReaderAdapterSuite) TestZlibEarlyError(c *C) {
	var compressedInput bytes.Buffer
	w := zlib.NewWriter(&compressedInput)
	_, _ = w.Write([]byte(wrself.testData))
	_ = w.Close()
	var finalOutput bytes.Buffer
	btf := fmt.Errorf("Born to fail")
	finalDecompressor := NewWriterToReaderAdapter(
		func(a io.Reader) (io.Reader, error) { return nil, btf },
		&finalOutput, true)
	skip := 1
	ci := compressedInput.Bytes()
	for len(ci) > 0 {
		if skip > len(ci) {
			skip = len(ci)
		}
		toCopy := ci[:skip]
		ci = ci[skip:]
		_, err := finalDecompressor.Write(toCopy)
		c.Assert(err, Equals, btf)
		break
	}

	err := finalDecompressor.Close()
	c.Assert(err, IsNil)
}

func (wrself *WriterToReaderAdapterSuite) TestZlibWriteTruncation(c *C) {
	var compressedInput bytes.Buffer
	w := zlib.NewWriter(&compressedInput)
	_, err := w.Write([]byte(wrself.testData + wrself.testData))
	c.Assert(err, IsNil)
	err = w.Close()
	c.Assert(err, IsNil)

	var finalOutput bytes.Buffer
	finalDecompressor := NewWriterToReaderAdapter(
		func(a io.Reader) (io.Reader, error) { return zlib.NewReader(a) },
		&finalOutput, true)
	skip := 1
	ci := compressedInput.Bytes()
	for len(ci) > 0 {
		// Note for posterity: If the last chunk to copy in is small and only at the end,
		// it's possible that the final string will be able to be reconstructed successfully.
		if skip >= len(ci) {
			break // don't deliver the last piece
		}
		toCopy := ci[:skip]
		ci = ci[skip:]
		_, err := finalDecompressor.Write(toCopy)
		c.Assert(err, IsNil)
		skip %= 128
		skip += 1
	}

	err = finalDecompressor.Close()
	c.Assert(err, Equals, io.ErrUnexpectedEOF)
	c.Assert(string(finalOutput.Bytes()) != (wrself.testData+wrself.testData), Equals, true)
}

var lateFailing error = fmt.Errorf("LateFailing")

type LateFailingZlibReader struct {
	rdr    io.ReadCloser
	count  int
	target int
}

func NewLateFailingZlibReader(input io.Reader, target int) (io.Reader, error) {
	rdr, err := zlib.NewReader(input)
	if err != nil {
		return nil, err
	}
	return &LateFailingZlibReader{rdr: rdr, target: target}, err
}
func (lfzself *LateFailingZlibReader) Read(buf []byte) (int, error) {
	lfzself.count += 1
	if lfzself.count > lfzself.target {
		return 0, lateFailing
	}
	return lfzself.rdr.Read(buf)
}

func (lfzself *LateFailingZlibReader) Close() error {
	return lfzself.rdr.Close()
}

func (wrself *WriterToReaderAdapterSuite) TestLateZlibFail(c *C) {
	var compressedInput bytes.Buffer
	w := zlib.NewWriter(&compressedInput)
	_, _ = w.Write([]byte(wrself.testData))
	_ = w.Close()
	var finalOutput bytes.Buffer
	finalDecompressor := NewWriterToReaderAdapter(
		func(input io.Reader) (io.Reader, error) {
			return NewLateFailingZlibReader(input, 2)
		},
		&finalOutput, true)
	skip := 1
	ci := compressedInput.Bytes()

	foundLateFailing := false
	for len(ci) > 0 {
		if skip >= len(ci) {
			break // don't deliver the last piece
		}
		toCopy := ci[:skip]
		ci = ci[skip:]
		_, err := finalDecompressor.Write(toCopy)
		if err == lateFailing {
			foundLateFailing = true
			break
		}

		skip %= 128
		skip += 1
	}
	c.Assert(foundLateFailing, Equals, true)
	err := finalDecompressor.Close()
	c.Assert(err, Equals, nil)
	c.Assert(string(finalOutput.Bytes()) != wrself.testData, Equals, true)
}

func (wrself *WriterToReaderAdapterSuite) TestEarlyZlibFail(c *C) {
	var compressedInput bytes.Buffer
	w := zlib.NewWriter(&compressedInput)
	_, _ = w.Write([]byte(wrself.testData))
	_ = w.Close()
	var finalOutput bytes.Buffer
	finalDecompressor := NewWriterToReaderAdapter(
		func(input io.Reader) (io.Reader, error) {
			return NewLateFailingZlibReader(input, -1)
		},
		&finalOutput, true)
	skip := 1
	ci := compressedInput.Bytes()

	foundLateFailing := false
	for len(ci) > 0 {
		if skip >= len(ci) {
			break // don't deliver the last piece
		}
		toCopy := ci[:skip]
		ci = ci[skip:]
		_, err := finalDecompressor.Write(toCopy)
		if err == lateFailing {
			foundLateFailing = true
			break
		}

		skip %= 128
		skip += 1
	}
	c.Assert(foundLateFailing, Equals, true)
	err := finalDecompressor.Close()
	c.Assert(err, Equals, nil)
	c.Assert(string(finalOutput.Bytes()) != wrself.testData, Equals, true)
}

// just reads one byte of the upstream reader and discards the rest
type ReadOneByte struct {
	Upstream io.Reader
}

func (robself *ReadOneByte) Read(buf []byte) (int, error) {
	if robself.Upstream == nil {
		return 0, io.EOF
	}
	upstream := robself.Upstream
	robself.Upstream = nil
	return upstream.Read(buf[:1])
}

func (wrself *WriterToReaderAdapterSuite) TestReadOneByteDrains(c *C) {
	var finalOutput bytes.Buffer
	writeOneByte := NewWriterToReaderAdapter(func(input io.Reader) (io.Reader, error) {
		return &ReadOneByte{Upstream: input}, nil
	}, &finalOutput, true)
	br, err := writeOneByte.Write([]byte{1, 2, 3, 4, 5})
	c.Assert(br, Equals, 5)
	c.Assert(err, Equals, nil)
	err = writeOneByte.Close()
	c.Assert(err, Equals, nil)
	c.Assert(string(finalOutput.Bytes()), Equals, string([]byte{1}))
}

var _ = Suite(&WriterToReaderAdapterSuite{})
